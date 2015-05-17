import functools
import logging
import time
from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult, Event
from gevent.queue import Queue

from scales.message import (
  OneWaySendCompleteMessage,
  ScalesErrorMessage,
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
from scales.varz import VarzReceiver

class TagPool(object):
  POOL_EXHAUSTED_VARZ = 'scales.thriftmux.TagPool.pool_exhausted'
  POOL_MAX_VARZ = 'scales.thriftmux.TagPool.max_tag'

  """A class which manages a pool of tags
  """
  def __init__(self, max_tag, varz_source):
    self._set = set()
    self._next = 1
    self._max_tag = max_tag
    self._varz_source = varz_source
    self.LOG = logging.getLogger('scales.thriftmux.TagPool.%s' % varz_source)


  def get(self):
    """Get a tag from the pool.

    Returns:
      A tag

    Raises:
      Exception if the next tag will be > max_tag
    """
    if not any(self._set):
      if self._next == self._max_tag - 1:
        VarzReceiver.IncrementVarz(self.POOL_EXHAUSTED_VARZ, self._varz_source, 1)
        raise Exception("No tags left in pool.")
      self._next += 1
      ret_tag = self._next
      self.LOG.debug('Allocating new tag, max is now %d' % ret_tag)
      VarzReceiver.SetVarz(self.POOL_MAX_VARZ, self._varz_source, ret_tag)
      return ret_tag
    else:
      return self._set.pop()

  def release(self, tag):
    """Return a tag to the pool.

    Args:
      tag - The previously leased tag.
    """
    if tag in self._set:
      self.LOG.warning('Tag %d has been returned more than once!' % tag)
    self._set.add(tag)


class ThriftMuxSocketTransportSink(ClientFormatterSink):
  BASE_TAG = 'scales.thriftmux.ThriftMuxSocketTransportSink'
  SEND_TAG = BASE_TAG + '.messages_sent'
  RECV_TAG = BASE_TAG + '.messages_recv'
  ACTIVE_TAG = BASE_TAG + '.active'
  TAG_FAILURES_TAG = BASE_TAG + '.tag_pool_exhausted'

  def __init__(self, socket, service):
    super(ThriftMuxSocketTransportSink, self).__init__()
    self._tag_map = {}
    self._socket = socket
    self._send_queue = Queue()
    self._ping_timeout = 5
    self._shutdown_ar = AsyncResult()
    self._ping_msg = self._BuildHeader(1, MessageType.Tping, 0)
    self._ping_ar = None
    self._last_ping_start = 0
    self._state = DispatcherState.UNINITIALIZED
    self._open_result = None
    self.LOG = logging.getLogger(
      'scales.thriftmux.ThriftMuxSocketTransportSink [%s:%d]' % (
        self._socket.host, self._socket.port))
    self._varz_source = '%s.%d' % (self._socket.host, self._socket.port)
    self._tag_pool = TagPool((2 ** 24) - 1, self._varz_source)

    increment_transport_varz = functools.partial(
        VarzReceiver.IncrementVarz, source=self._varz_source)
    increment_service_varz = functools.partial(
        VarzReceiver.IncrementVarz, source=service)
    def _increment_varz(metric, amount=1):
      increment_transport_varz(metric, amount=amount)
      increment_service_varz(metric, amount=amount)
    self._increment_varz = _increment_varz

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
      self.LOG.debug('Opening transport.')
      self._socket.open()
      gevent.spawn(self._RecvLoop)
      gevent.spawn(self._SendLoop)

      self._state = DispatcherState.STARTING
      ar = self._SendPingMessage()
      ar.get()
      self.LOG.debug('Open and ping successful')
      gevent.spawn(self._PingLoop)
      self._state = DispatcherState.RUNNING
      self._open_result.set()
      self._increment_varz(self.ACTIVE_TAG)
    except Exception as e:
      self.LOG.error('Exception opening socket')
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

    self.LOG.warning('Shutting down transport [%s].' % str(reason))
    self._increment_varz(self.ACTIVE_TAG, -1)
    self._state = DispatcherState.STOPPED
    self._socket.close()
    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    self._shutdown_ar.set_exception(reason)
    msg = ScalesErrorMessage(reason)

    for sink_stack in self._tag_map.values():
      sink_stack.DispatchReplyMessage(msg)

  def _SendPingMessage(self):
    """Constucts and sends a Tping message.
    """
    self.LOG.debug('Sending ping message.')
    self._ping_ar = AsyncResult()
    self._last_ping_start = time.time()
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
      ping_duration = time.time() - self._last_ping_start
      self.LOG.debug('Got ping response in %d ms' % int(ping_duration * 1000))
    else:
      self.LOG.error('Unexpected response for tag 1 (msg_type was %d)' % msg_type)
      ar.set_exception(Exception("Invalid ping response"))

  def _PingLoop(self):
    """Periodically pings the remote server.
    """
    while self.isActive:
      gevent.sleep(30)
      if self.isActive:
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
        self._increment_varz(self.SEND_TAG)
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
        self._increment_varz(self.RECV_TAG)
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
        reply_stack = self._ReleaseTag(tag)
        if reply_stack:
          stream.seek(0)
          reply_stack.AsyncProcessResponse(stream)
    except Exception:
      self.LOG.exception('Exception processing reply message.')

  def _ReleaseTag(self, tag):
    """Return a tag to the tag pool.

    Note: Tags are only returned when the server has ACK'd them (or NACK'd) with
    and Rdispatch message (or similar).  Client initiated timeouts do NOT return
    tags to the pool.

    Args:
      tag - The tag to return.

    Returns:
      The ClientChannelSinkStack associated with the tag's response.
    """
    reply_stack = self._tag_map.pop(tag, None)
    self._tag_pool.release(tag)
    return reply_stack

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
    if not msg.is_one_way:
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
  BASE_VARZ = 'scales.thriftmux.ThrfitMuxMessageSerializerSink'
  DESERIALIZATION_FAILURES_VARZ = BASE_VARZ + '.deserialization_failures'
  SERIALIZATION_FAILURES_VARZ = BASE_VARZ + '.serialization_failures'

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

  def __init__(self, varz_source):
    super(ThrfitMuxMessageSerializerSink, self).__init__()
    self._serializer = MessageSerializer()
    self._reply_sink = None
    self._varz_source = varz_source
    self._increment_varz = functools.partial(
        VarzReceiver.IncrementVarz, source=varz_source, amount=1)

  def AsyncProcessMessage(self, msg, reply_sink):
    fpb = StringIO()
    try:
      ctx = self._serializer.Marshal(msg, fpb)
    except Exception as ex:
      self._increment_varz(self.SERIALIZATION_FAILURES_VARZ)
      msg = ScalesErrorMessage(ex)
      reply_sink.ProcessReturnMessage(msg)
      return

    if msg.is_one_way:
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
      self._increment_varz(self.DESERIALIZATION_FAILURES_VARZ)
      msg = ScalesErrorMessage(ex)
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
  def __init__(self, varz_source):
    super(TimeoutSink, self).__init__()
    self._varz_source = varz_source

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

