import traceback

from collections import deque
from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue

from src.message import (
    RdispatchMessage,
    RerrorMessage,
    TdiscardedMessage,
    ShutdownMessage,
    MessageSerializer)
from src.ttypes import MessageType

class DispatcherException(Exception): pass
class ServerException(Exception):  pass
class TimeoutException(Exception): pass

class TagPool(object):
  """A class which manages a pool of tags
  """
  def __init__(self, max_tag):
    self._set = set()
    self._next = 1
    self._max_tag = max_tag

  def get(self):
    """Get a tag from the pool.

    Returns:
      A tag

    Raises:
      Exception if the next tag will be > max_tag
    """
    if not any(self._set):
      if self._next == self._max_tag - 1:
        raise Exception("No tags left in pool.")

      self._next += 1
      return self._next
    else:
      return self._set.pop()

  def release(self, tag):
    """Return a tag to the pool.

    Args:
      tag - The previously leased tag.
    """
    self._set.add(tag)


class ClientSink(object):
  def __init__(self):
    self._next = None

  @property
  def next_sink(self):
    return self._next

  @next_sink.setter
  def next_sink(self, value):
    self._next = value

  def AsyncProcessMessage(self, msg, stream, reply_sink):
    self.next_sink.AsyncProcessMessage(msg, stream, reply_sink)


class ReplySink(object):
  def __init__(self):
    self._reply_sink = None

  @property
  def reply_sink(self):
    return self._reply_sink

  @reply_sink.setter
  def reply_sink(self, value):
    self._reply_sink = value

  def ProcessReply(self, msg, stream):
    self._reply_sink.ProcessReply(msg, stream)


class SinkStack(object):
  def __init__(self):
    self._stack = deque()

  def Push(self, sink):
    self._stack.append(sink)

  def Pop(self):
    self._stack.pop()

class ThriftMuxSocketTransportSink(ClientSink):
  def __init__(self, socket):
    super(ThriftMuxSocketTransportSink, self).__init__()
    self._socket = socket
    self._send_queue = Queue()
    gevent.spawn(self._SendLoop)
    gevent.spawn(self._RecvLoop)
    self._tag_pool = TagPool((2 ** 24) - 1)
    self._tag_map = {}
    self._shutdown_ar = AsyncResult()

  def _ReleaseTag(self, tag):
    """Return a tag to the tag pool.

    Note: Tags are only returned when the server has ACK'd them (or NACK'd) with
    and Rdispatch message (or similar).  Client initiated timeouts do NOT return
    tags to the pool.

    Args:
      tag - The tag to return.

    Returns:
      The AsyncResult associated with the tag's response.
    """
    ar = self._tag_map.pop(tag, None)
    self._tag_pool.release(tag)
    return ar

  @staticmethod
  def _EncodeTag(tag):
    return [tag >> 16 & 0xff, tag >> 8 & 0xff, tag & 0xff] # Tag

  def _BuildHeader(self, tag, msg_type, data_len):
    total_len = 1 + 3 + data_len
    return pack('!ibBBB',
      total_len,
      msg_type,
      *self._EncodeTag(tag))

  def _SendLoop(self):
    """Dispatch messages from the send queue to the remote server.

    Note: Messages in the queue have already been serialized into wire format.
    """
    while True:
      try:
        header, payload = self._send_queue.get()
        self._socket.write(header + payload.getvalue())
      except Exception as e:
        self._shutdown_ar.set_exception(e)
        break

  def _RecvLoop(self):
    """Dispatch messages from the remote server to their recipient.

    Note: Deserialization and dispatch occurs on a seperate greenlet, this only
    reads the message off the wire.
    """
    while True:
      try:
        sz, = unpack('!i', self._socket.readAll(4))
        buf = StringIO(self._socket.readAll(sz))
        gevent.spawn(self._ProcessReply, None, buf)
      except Exception as e:
        self._shutdown_ar.set_exception(e)
        break

  def AsyncProcessMessage(self, msg, stream, reply_sink):
    if reply_sink:
      tag = self._tag_pool.get()
      self._tag_map[tag] = reply_sink
    else:
      tag = 0

    data_len = stream.tell()
    header = self._BuildHeader(tag, msg.type, data_len)
    self._send_queue.put((header, stream))
    return None

  def _ProcessReply(self, msg, stream):
    try:
      _, tag = ThrfitMuxMessageSerializerSink.ReadHeader(stream)
      stream.seek(0)
      if tag != 0:
        reply_sink = self._tag_map.get(tag)
        if reply_sink:
          reply_sink.ProcessReply(msg, stream)
    except Exception:
      pass


class ThrfitMuxMessageSerializerSink(ClientSink, ReplySink):
  """Serialize a ThriftMux Message to a stream
  """
  @staticmethod
  def ReadHeader(stream):
    """Read a mux header off a message.

    Args:
      msg - a byte buffer of raw data.

    Returns:
      A tuple of (message_type, tag)
    """
    header, = unpack('!i', stream.read(4))
    msg_type = (256 - (header >> 24 & 0xff)) * -1
    tag = ((header << 8) & 0xFFFFFFFF) >> 8
    return msg_type, tag

  def ProcessReply(self, msg, stream):
    try:
      msg_type, tag = ThrfitMuxMessageSerializerSink.ReadHeader(stream)
      msg = self._serializer.Unmarshal(tag, msg_type, stream)
    except Exception:
      why = traceback.format_exc()
      msg = RerrorMessage(why)

    return self.reply_sink.ProcessReply(msg, stream)

  def __init__(self):
    super(ThrfitMuxMessageSerializerSink, self).__init__()
    self._serializer = MessageSerializer()
    self._reply_sink = None

  def AsyncProcessMessage(self, msg, stream, reply_sink):
    fpb = StringIO()
    msg.Marshal(fpb)
    self.reply_sink = reply_sink
    return self.next_sink.AsyncProcessMessage(
        msg,
        fpb,
        self)

class TimeoutSink(ClientSink):
  def _TimeoutHelper(self, ar, timeout, tag):
    """Waits for ar to be signaled or [timeout] seconds to elapse.  If the
    timeout elapses, a Tdiscarded message will be queued to the server indicating
    the client is no longer expecting a reply.
    """
    ar.wait(timeout)
    if not ar.ready():
      ar.set_exception(TimeoutException('The thrift call did not complete within the specified timeout and has been aborted.'))
      msg = TdiscardedMessage(tag, 'timeout')


  def _InitTimeout(self, timeout, tag, reply_sink):
    """Initialize the timeout handler for this request.

    Args:
      ar - The AsyncResult for the pending response of this request.
      timeout - An optional timeout.  If None, no timeout handler is initialized.
      tag - The tag of the request.
    """
    if timeout:
      gevent.spawn(self._TimeoutHelper, timeout, tag)

  def AsyncProcessMessage(self, msg, stream, reply_sink):
    self._InitTimeout(msg.timeout, msg.tag, reply_sink)


class ThriftMuxDispatchSink(ClientSink, ReplySink):
  def _CompleteCall(self, msg):
    if self.reply_sink:
      self.reply_sink.ProcessReply(msg, None)

  def _Tping(self, msg):
    pass

  def _Rping(self, msg):
    self._CompleteCall(msg)

  def _Rdispatch(self, msg):
    self._CompleteCall(msg)

  def __init__(self):
    super(ThriftMuxDispatchSink, self).__init__()
    self._dispatchers = {
      MessageType.Rdispatch: self._Rdispatch,
      MessageType.Rping: self._Rping,

      MessageType.Tping: self._Tping,
    }

  def ProcessReply(self, msg, stream):
    reply_dispatcher = self._dispatchers.get(msg.type)
    if reply_dispatcher:
      reply_dispatcher(msg)
    else:
      error_msg = RerrorMessage(
        'No dispatch handler was found for message type %s.' % msg.type)
      self.reply_sink.ProcessReply(error_msg)

  def AsyncProcessMessage(self, msg, stream, reply_sink):
    if reply_sink:
      dispatch_reply_sink = self
    else:
      dispatch_reply_sink = None

    self.reply_sink = reply_sink
    self.next_sink.AsyncProcessMessage(msg, stream, dispatch_reply_sink)


class GeventReplySink(ReplySink):
  def __init__(self):
    super(GeventReplySink, self).__init__()
    self._ar = AsyncResult()

  def ProcessReply(self, msg, stream):
    if isinstance(msg, RdispatchMessage):
      if msg.error:
        self._ar.set_exception(ServerException(msg.error))
      else:
        self._ar.set(msg.response)
    else:
      self._ar.set(msg)

  @property
  def async_result(self):
    return self._ar
