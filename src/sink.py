import traceback

from collections import deque
from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult, Event
from gevent.queue import Queue

from src.message import (
    RdispatchMessage,
    RerrorMessage,
    TdiscardedMessage,
    MessageSerializer,
    TimeoutMessage,
    Timeout,
    Tag)

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


class MessageSink(object):
  def __init__(self):
    super(MessageSink, self).__init__()
    self._next = None

  @property
  def next_sink(self):
    return self._next

  @next_sink.setter
  def next_sink(self, value):
    self._next = value


class SyncMessageSink(MessageSink):
  def __init__(self):
    super(SyncMessageSink, self).__init__()

  def SyncProcessMessage(self, msg):
    raise NotImplementedError()


class AsyncMessageSink(MessageSink):
  def __init__(self):
    super(AsyncMessageSink, self).__init__()

  def AsyncProcessMessage(self, msg, reply_sink):
    raise NotImplementedError()


class ClientChannelSink(object):
  def __init__(self):
    super(ClientChannelSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream):
    raise NotImplementedError()

  def AsyncProcessResponse(self, sink_stack, stream):
    raise NotImplementedError()


class ClientFormatterSink(AsyncMessageSink, ClientChannelSink):
  def __init__(self):
    super(ClientFormatterSink, self).__init__()


class SinkStack(object):
  def __init__(self):
    self._stack = deque()

  def Push(self, sink):
    self._stack.append(sink)

  def Pop(self):
    return self._stack.pop()


class ClientChannelSinkStack(SinkStack):
  def __init__(self, reply_sink):
    super(ClientChannelSinkStack, self).__init__()
    self._reply_sink = reply_sink

  def AsyncProcessResponse(self, stream):
    if self._reply_sink:
      sink = self.Pop()
      sink.AsyncProcessResponse(self, stream)

  def DispatchReplyMessage(self, msg):
    if self._reply_sink:
      self._reply_sink.SyncProcessMessage(msg)


class MessageProcessing(object):
  def Cancel(self):
    pass


class ThriftMuxSocketTransportSink(ClientFormatterSink):
  def __init__(self, socket):
    super(ThriftMuxSocketTransportSink, self).__init__()
    self._tag_pool = TagPool((2 ** 24) - 1)
    self._tag_map = {}
    self._socket = socket
    self._send_queue = Queue()
    gevent.spawn(self._SendLoop)
    gevent.spawn(self._RecvLoop)
    self._shutdown_ar = AsyncResult()

  def _SendLoop(self):
    """Dispatch messages from the send queue to the remote server.

    Note: Messages in the queue have already been serialized into wire format.
    """
    while True:
      try:
        payload = self._send_queue.get()
        self._socket.write(payload)
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
        gevent.spawn(self._ProcessReply, buf)
      except Exception as e:
        self._shutdown_ar.set_exception(e)
        break

  def _ProcessReply(self, stream):
    try:
      _, tag = ThrfitMuxMessageSerializerSink.ReadHeader(stream)
      stream.seek(0)
      if tag != 0:
        reply_stack = self._tag_map.get(tag)
        if reply_stack:
          reply_stack.AsyncProcessResponse(stream)
    except Exception:
      pass

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

  def AsyncProcessRequest(self, sink_stack, msg, stream):
    if sink_stack:
      tag = self._tag_pool.get()
      msg.properties[Tag.KEY] = tag
      self._tag_map[tag] = sink_stack
    else:
      tag = 0

    data_len = stream.tell()
    header = self._BuildHeader(tag, msg.type, data_len)
    payload = header + stream.getvalue()
    self._send_queue.put(payload)

  def AsyncProcessResponse(self, sink_stack, stream):
    raise Exception("Not supported.")


class ThrfitMuxMessageSerializerSink(ClientFormatterSink):
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

  def __init__(self):
    super(ThrfitMuxMessageSerializerSink, self).__init__()
    self._serializer = MessageSerializer()
    self._reply_sink = None

  def AsyncProcessMessage(self, msg, reply_sink):
    fpb = StringIO()
    msg.Marshal(fpb)
    sink_stack = ClientChannelSinkStack(reply_sink)
    sink_stack.Push(self)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, fpb)

  def AsyncProcessRequest(self, sink_stack, msg, stream):
    raise Exception("Not supported")

  def AsyncProcessResponse(self, sink_stack, stream):
    try:
      msg_type, tag = ThrfitMuxMessageSerializerSink.ReadHeader(stream)
      msg = self._serializer.Unmarshal(tag, msg_type, stream)
    except Exception:
      why = traceback.format_exc()
      msg = RerrorMessage(why)

    sink_stack.DispatchReplyMessage(msg)


class TimeoutReplySink(SyncMessageSink):
  def __init__(self, client_sink, msg, next_sink, timeout):
    super(TimeoutReplySink, self).__init__()
    self.next_sink = next_sink
    self._call_done = Event()
    self._msg = msg
    self._client_sink = client_sink
    gevent.spawn(self._TimeoutHelper, timeout)

  def SyncProcessMessage(self, msg):
    self._call_done.set()
    if self.next_sink:
      self.next_sink.SyncProcessMessage(msg)

  def _TimeoutHelper(self, timeout):
    """Waits for ar to be signaled or [timeout] seconds to elapse.  If the
    timeout elapses, a Tdiscarded message will be queued to the server indicating
    the client is no longer expecting a reply.
    """
    self._call_done.wait(timeout)
    if not self._call_done.is_set() and self.next_sink:
      error_msg = TimeoutMessage()
      reply_sink, self.next_sink = self.next_sink, None
      reply_sink.SyncProcessMessage(error_msg)

      tag = self._msg.properties.get(Tag.KEY)
      if tag:
        msg = TdiscardedMessage(tag, 'Client Timeout')
        self._client_sink.AsyncProcessMessage(msg, None)


class TimeoutSink(AsyncMessageSink):
  def AsyncProcessMessage(self, msg, reply_sink):
    """Initialize the timeout handler for this request.

    Args:
      ar - The AsyncResult for the pending response of this request.
      timeout - An optional timeout.  If None, no timeout handler is initialized.
      tag - The tag of the request.
    """
    timeout = msg.properties.get(Timeout.KEY)
    if timeout:
      reply_sink = TimeoutReplySink(self, msg, reply_sink, timeout)
    return self.next_sink.AsyncProcessMessage(msg, reply_sink)


class GeventMessageTerminatorSink(SyncMessageSink):
  def __init__(self):
    super(GeventMessageTerminatorSink, self).__init__()
    self._ar = AsyncResult()

  def SyncProcessMessage(self, msg):
    if isinstance(msg, RdispatchMessage):
      if msg.error:
        self._ar.set_exception(ServerException(msg.error))
      else:
        self._ar.set(msg.response)
    elif isinstance(msg, TimeoutMessage):
      self._ar.set_exception(TimeoutException(
          'The call did not complete within the specified timeout '
          'and has been aborted.'))
    else:
      self._ar.set(msg)

  @property
  def async_result(self):
    return self._ar
