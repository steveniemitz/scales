from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult, Event
from gevent.queue import Queue

from scales.message import (
  OneWaySendCompleteMessage,
  SystemErrorMessage,
  TimeoutMessage,
  Timeout,
  MessageType,
  TdiscardedMessage,
)

from scales.tmux.formatter import (
  MessageSerializer,
  Tag
)

from scales.sink import (
  AsyncMessageSink,
  ClientChannelSinkStack,
  ClientFormatterSink,
  ReplySink,
)

from scales.ttypes import DispatcherState

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


class ThriftMuxSocketTransportSink(ClientFormatterSink):
  def __init__(self, socket):
    super(ThriftMuxSocketTransportSink, self).__init__()
    self._tag_pool = TagPool((2 ** 24) - 1)
    self._tag_map = {}
    self._socket = socket
    self._send_queue = Queue()
    self._ping_timeout = 5
    self._shutdown_ar = AsyncResult()
    self._ping_msg = self._BuildHeader(1, MessageType.Tping, 0)
    self._ping_ar = None
    self._state = DispatcherState.UNINITIALIZED
    self._open_result = None

  @property
  def shutdown_result(self):
    return self._shutdown_ar

  @property
  def isActive(self):
    return self._state != DispatcherState.STOPPED

  def open(self):
    """Initializes the dispatcher, opening a connection to the remote host.
    This method may only be called once.
    """
    if self._state == DispatcherState.RUNNING:
      return
    elif self._open_result:
      self._open_result.get()
      return

    self._open_result = AsyncResult()
    try:
      self._socket.open()
      gevent.spawn(self._RecvLoop)
      gevent.spawn(self._SendLoop)

      self._state = DispatcherState.STARTING
      ar = self._SendPingMessage()
      ar.get()
      gevent.spawn(self._PingLoop)
      self._state = DispatcherState.RUNNING
      self._open_result.set()
    except Exception as e:
      self._open_result.set_exception(e)
      raise

  def close(self):
    self._Shutdown('close invoked')

  def isOpen(self):
    return self._state == DispatcherState.RUNNING

  def testConnection(self):
    if not self._state == DispatcherState.RUNNING:
      return False
    if hasattr(self._socket, 'testConnection'):
      return self._socket.testConnection()
    else:
      return True

  def _Shutdown(self, reason):
    if not self.isActive:
      return

    self._state = DispatcherState.STOPPED
    self._socket.close()
    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    self._shutdown_ar.set_exception(reason)
    msg = SystemErrorMessage(reason)

    for sink_stack in self._tag_map.values():
      sink_stack.DispatchReplyMessage(msg)

  def _SendPingMessage(self):
    """Constucts and sends a Tping message.
    """
    self._ping_ar = AsyncResult()
    self._send_queue.put(self._ping_msg)
    gevent.spawn(self._PingTimeoutHelper)
    return self._ping_ar

  def _PingTimeoutHelper(self):
    ar = self._ping_ar
    ar.wait(self._ping_timeout)
    if not ar.successful():
      self._Shutdown('Ping Timeout')

  def _OnPingResponse(self, msg_type, stream):
    """Handles the response to a ping.  On failure, shuts down the dispatcher.
    """
    ar, self._ping_ar = self._ping_ar, None
    if msg_type == MessageType.Rping:
      ar.set()
    else:
      ar.set_exception(Exception("Invalid ping response"))

  def _PingLoop(self):
    """Periodically pings the remote server.
    """
    while self.isActive:
      gevent.sleep(30)
      if not self.isActive:
        self._SendPingMessage()
      else:
        break

  def _SendLoop(self):
    """Dispatch messages from the send queue to the remote server.

    Note: Messages in the queue have already been serialized into wire format.
    """
    while self.isActive:
      try:
        payload = self._send_queue.get()
        self._socket.write(payload)
      except Exception as e:
        self._Shutdown(e)
        break

  def _RecvLoop(self):
    """Dispatch messages from the remote server to their recipient.

    Note: Deserialization and dispatch occurs on a seperate greenlet, this only
    reads the message off the wire.
    """
    while self.isActive:
      try:
        sz, = unpack('!i', self._socket.readAll(4))
        buf = StringIO(self._socket.readAll(sz))
        gevent.spawn(self._ProcessReply, buf)
      except Exception as e:
        self._Shutdown(e)
        break

  def _ProcessReply(self, stream):
    try:
      msg_type, tag = ThrfitMuxMessageSerializerSink.ReadHeader(stream)
      if tag == 1: #Ping
        self._OnPingResponse(msg_type, stream)
      elif tag != 0:
        stream.seek(0)
        reply_stack = self._tag_map.get(tag)
        if reply_stack:
          reply_stack.AsyncProcessResponse(stream)
    except Exception:
      #TODO: log
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
    if True:
      tag = self._tag_pool.get()
      msg.properties[Tag.KEY] = tag
      self._tag_map[tag] = sink_stack
    else:
      tag = 0

    data_len = stream.tell()
    header = self._BuildHeader(tag, msg.type, data_len)
    payload = header + stream.getvalue()
    self._send_queue.put(payload)

  def AsyncProcessResponse(self, sink_stack, context, stream):
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
    is_one_way, ctx = self._serializer.Marshal(msg, fpb)

    if is_one_way:
      one_way_reply_sink, reply_sink = reply_sink, None
    else:
      one_way_reply_sink = None

    sink_stack = ClientChannelSinkStack(reply_sink)
    sink_stack.Push(self, ctx)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, fpb)
    # The call is one way, so ignore the response.
    if one_way_reply_sink:
      one_way_reply_sink.ProcessReturnMessage(OneWaySendCompleteMessage())

  def AsyncProcessRequest(self, sink_stack, msg, stream):
    raise Exception("Not supported")

  def AsyncProcessResponse(self, sink_stack, context, stream):
    try:
      msg_type, tag = ThrfitMuxMessageSerializerSink.ReadHeader(stream)
      msg = self._serializer.Unmarshal(tag, msg_type, stream, context)
    except Exception as ex:
      msg = SystemErrorMessage(ex)
    sink_stack.DispatchReplyMessage(msg)


class TimeoutReplySink(ReplySink):
  def __init__(self, client_sink, msg, next_sink, timeout):
    super(TimeoutReplySink, self).__init__()
    self.next_sink = next_sink
    self._call_done = Event()
    self._msg = msg
    self._client_sink = client_sink
    gevent.spawn(self._TimeoutHelper, timeout)

  def ProcessReturnMessage(self, msg):
    self._call_done.set()
    if self.next_sink:
      self.next_sink.ProcessReturnMessage(msg)

  def _TimeoutHelper(self, timeout):
    """Waits for ar to be signaled or [timeout] seconds to elapse.  If the
    timeout elapses, a Tdiscarded message will be queued to the server indicating
    the client is no longer expecting a reply.
    """
    self._call_done.wait(timeout)
    if not self._call_done.is_set() and self.next_sink:
      error_msg = TimeoutMessage()
      reply_sink, self.next_sink = self.next_sink, None
      reply_sink.ProcessReturnMessage(error_msg)

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

