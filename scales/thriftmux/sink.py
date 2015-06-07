import logging
import time
from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue

from ..constants import (ChannelState, SinkProperties)
from ..message import (
  ClientError,
  Deadline,
  MethodDiscardMessage,
  MethodReturnMessage
)
from ..sink import (
  ClientMessageSink,
  ChannelSinkProvider,
  ChannelSinkProviderBase,
)
from ..thrift.socket import TSocket
from ..varz import (
  AggregateTimer,
  AverageTimer,
  Counter,
  Gauge,
  SourceType,
  VarzBase,
  VarzSocketWrapper
)
from .formatter import (
  MessageSerializer,
  Tag
)
from .protocol import (
  Headers,
  MessageType,
)

ROOT_LOG = logging.getLogger('scales.thriftmux')

class TagPool(object):
  """A class which manages a pool of tags
    """
  POOL_LOGGER = ROOT_LOG.getChild('TagPool')

  class Varz(VarzBase):
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ_BASE_NAME = 'scales.thriftmux.TagPool'
    _VARZ = {
      'pool_exhausted': Counter,
      'max_tag': Gauge
    }

  def __init__(self, max_tag, service, host):
    self._set = set()
    self._next = 1
    self._max_tag = max_tag
    self._varz = self.Varz((service, host))
    self._log = self.POOL_LOGGER.getChild('[%s.%s]' % (service, host))

  def get(self):
    """Get a tag from the pool.

    Returns:
      A tag

    Raises:
      Exception if the next tag will be > max_tag
    """
    if not any(self._set):
      if self._next == self._max_tag - 1:
        self._varz.pool_exhausted()
        raise Exception("No tags left in pool.")
      self._next += 1
      ret_tag = self._next
      self._log.debug('Allocating new tag, max is now %d' % ret_tag)
      self._varz.max_tag(ret_tag)
      return ret_tag
    else:
      return self._set.pop()

  def release(self, tag):
    """Return a tag to the pool.

    Args:
      tag - The previously leased tag.
    """
    if tag in self._set:
      self._log.warning('Tag %d has been returned more than once!' % tag)
    self._set.add(tag)


class SocketTransportSink(ClientMessageSink):
  """A transport sink for thriftmux servers."""

  SINK_LOG = ROOT_LOG.getChild('SocketTransportSink')
  _EMPTY_DCT = {}

  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.thriftmux.SocketTransportSink'
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'messages_sent': Counter,
      'messages_recv': Counter,
      'active': Gauge,
      'send_queue_size': Gauge,
      'send_time': AggregateTimer,
      'recv_time': AggregateTimer,
      'send_latency': AverageTimer,
      'recv_latency': AverageTimer,
      'transport_latency': AverageTimer
    }

  def __init__(self, socket, service):
    super(SocketTransportSink, self).__init__()
    self._socket = socket
    self._ping_timeout = 5
    self._ping_msg = self._BuildHeader(1, MessageType.Tping, 0)
    self._last_ping_start = 0
    self._state = ChannelState.Idle
    self._log = self.SINK_LOG.getChild('[%s.%s:%d]' % (
        service, self._socket.host, self._socket.port))
    self._socket_source = '%s:%d' % (self._socket.host, self._socket.port)
    self._service = service
    self._open_result = None

  def _Init(self):
    self._tag_map = {}
    self._open_result = None
    self._ping_ar = None
    self._tag_pool = TagPool((2 ** 24) - 1, self._service, self._socket_source)
    self._varz = self.Varz((self._service, self._socket_source))
    self._greenlets = []
    self._send_queue = Queue()

  @property
  def isActive(self):
    return self._state != ChannelState.Closed

  @property
  def state(self):
    return self._state

  def Open(self, force=False):
    """Initializes the dispatcher, opening a connection to the remote host.
    This method may only be called once.
    """
    if self._state == ChannelState.Open:
      return
    elif self._open_result:
      self._open_result.get()
      return

    self._Init()
    self._open_result = AsyncResult()
    try:
      self._log.debug('Opening transport.')
      self._socket.open()
      self._greenlets.append(gevent.spawn(self._RecvLoop))
      self._greenlets.append(gevent.spawn(self._SendLoop))

      ar = self._SendPingMessage()
      ar.get()
      self._log.debug('Open and ping successful')
      self._greenlets.append(gevent.spawn(self._PingLoop))
      self._state = ChannelState.Open
      self._open_result.set()
      self._varz.active(1)
    except Exception as e:
      self._log.error('Exception opening socket')
      self._open_result.set_exception(e)
      self._Shutdown('Open failed')
      raise

  def Close(self):
    self._Shutdown('Close invoked', False)

  def _Shutdown(self, reason, fault=True):
    if not self.isActive:
      return

    self._state = ChannelState.Closed
    self._log.warning('Shutting down transport [%s].' % str(reason))
    self._varz.active(0)
    self._socket.close()
    [g.kill(block=False) for g in self._greenlets]
    self._greenlets = []

    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    if fault:
      self.on_faulted.Set(reason)
    msg = MethodReturnMessage(error=ClientError(reason))

    for sink_stack, _, _ in self._tag_map.values():
      sink_stack.AsyncProcessResponseMessage(msg)

    self._tag_map = {}
    self._open_result = AsyncResult()
    self._send_queue = Queue()

  def _SendPingMessage(self):
    """Constucts and sends a Tping message.
    """
    self._log.debug('Sending ping message.')
    self._ping_ar = AsyncResult()
    self._last_ping_start = time.time()
    self._send_queue.put((self._ping_msg, self._EMPTY_DCT))
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
      self._log.debug('Got ping response in %d ms' % int(ping_duration * 1000))
    else:
      self._log.error('Unexpected response for tag 1 (msg_type was %d)' % msg_type)
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

  def _HandleTimeout(self, dct):
    timeout_event = dct.get(Deadline.EVENT_KEY, None)
    if timeout_event and timeout_event.Get():
      # The message has timed out before it got out of the send queue
      # In this case, we can discard it immediatly without even sending it.
      tag = dct.pop(Tag.KEY, 0)
      if tag != 0:
        self._ReleaseTag(tag)
      return True
    elif timeout_event:
      # The event exists but hasn't been signaled yet, hook up a
      # callback so we can be notified on a timeout.
      timeout_event.Subscribe(lambda evt: self._OnTimeout(dct), True)
      return False
    else:
      # No event was created, so this will never timeout.
      return False

  @staticmethod
  def _CreateDiscardMessage(tag):
    discard_message = MethodDiscardMessage(tag, 'Client timeout')
    discard_message.which = tag
    buf = StringIO()
    headers = {}
    MessageSerializer().Marshal(discard_message, buf, headers)
    return discard_message, buf, headers

  def _OnTimeout(self, dct):
    tag = dct.pop(Tag.KEY, 0)
    if tag:
      msg, buf, headers = self._CreateDiscardMessage(tag)
      self.AsyncProcessRequest(None, msg, buf, headers)

  def _SendLoop(self):
    """Dispatch messages from the send queue to the remote server.

    Note: Messages in the queue have already been serialized into wire format.
    """
    while self.isActive:
      try:
        payload, dct = self._send_queue.get()
        # HandleTimeout sets up the transport level timeout handling
        # for this message.  If the message times out in transit, this
        # transport will handle sending a Tdiscarded to the server.
        if self._HandleTimeout(dct): continue

        queue_len = self._send_queue.qsize()
        self._varz.send_queue_size(queue_len)
        with self._varz.send_time.Measure():
          with self._varz.send_latency.Measure():
            self._socket.write(payload)
        self._varz.messages_sent()
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
        with self._varz.recv_time.Measure():
          with self._varz.recv_latency.Measure():
            buf = StringIO(self._socket.readAll(sz))
        self._varz.messages_recv()
        gevent.spawn(self._ProcessReply, buf)
      except Exception as e:
        self._Shutdown(e)
        break

  def _ProcessReply(self, stream):
    try:
      msg_type, tag = ThriftMuxMessageSerializerSink.ReadHeader(stream)
      if tag == 1 and msg_type == MessageType.Rping: #Ping
        self._OnPingResponse(msg_type, stream)
      elif tag != 0:
        tup = self._ReleaseTag(tag)
        if tup:
          reply_stack, start_time, props = tup
          props[Tag.KEY] = None
          self._varz.transport_latency(time.time() - start_time)
          stream.seek(0)
          reply_stack.AsyncProcessResponseStream(stream)
      else:
        self._log.error('Unexpected message, msg_type = %d, tag = %d' % (msg_type, tag))
    except Exception:
      self._log.exception('Exception processing reply message.')

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
    tup = self._tag_map.pop(tag, None)
    self._tag_pool.release(tag)
    return tup

  @staticmethod
  def _EncodeTag(tag):
    return [tag >> 16 & 0xff, tag >> 8 & 0xff, tag & 0xff] # Tag

  def _BuildHeader(self, tag, msg_type, data_len):
    total_len = 1 + 3 + data_len
    return pack('!ibBBB',
                total_len,
                msg_type,
                *self._EncodeTag(tag))

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if not msg.is_one_way:
      tag = self._tag_pool.get()
      msg.properties[Tag.KEY] = tag
      self._tag_map[tag] = (sink_stack, time.time(), msg.properties)
    else:
      tag = 0

    data_len = stream.tell()
    header = self._BuildHeader(tag, headers[Headers.MessageType], data_len)
    payload = header + stream.getvalue()
    self._send_queue.put((payload, msg.properties))

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass

class ThriftMuxMessageSerializerSink(ClientMessageSink):
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.thriftmux.ThrfitMuxMessageSerializerSink'
    _VARZ = {
      'deserialization_failures': Counter,
      'serialization_failures': Counter
    }

  def __init__(self, next_provider, properties):
    super(ThriftMuxMessageSerializerSink, self).__init__()
    self.next_sink = next_provider.CreateSink(properties)
    self._serializer = MessageSerializer()
    self._varz = self.Varz(properties[SinkProperties.Service])

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

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    buf = StringIO()
    headers = {}

    deadline = msg.properties.get(Deadline.KEY, None)
    if deadline:
      headers['com.twitter.finagle.Deadline'] = Deadline(deadline)

    try:
      ctx = self._serializer.Marshal(msg, buf, headers)
    except Exception as ex:
      self._varz.serialization_failures()
      msg = MethodReturnMessage(error=ex)
      sink_stack.AsyncProcessResponseMessage(msg)
      return

    sink_stack.Push(self, ctx)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, buf, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    if msg:
      sink_stack.AsyncProcessResponseMessage(msg)
    else:
      try:
        msg_type, tag = ThriftMuxMessageSerializerSink.ReadHeader(stream)
        msg = self._serializer.Unmarshal(tag, msg_type, stream, context)
      except Exception as ex:
        self._varz.deserialization_failures()
        msg = MethodReturnMessage(error=ex)
      sink_stack.AsyncProcessResponseMessage(msg)

ThriftMuxMessageSerializerSinkProvider = ChannelSinkProvider(ThriftMuxMessageSerializerSink)

class SocketTransportSinkProvider(ChannelSinkProviderBase):
  def CreateSink(self, properties):
    server = properties[SinkProperties.Endpoint]
    service = properties[SinkProperties.Service]

    sock = TSocket.TSocket(server.host, server.port)
    healthy_sock = VarzSocketWrapper(sock, service)
    socket_sink = SocketTransportSink(healthy_sock, service)
    return socket_sink

  @property
  def sink_class(self):
    return SocketTransportSink