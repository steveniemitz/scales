from struct import (pack, unpack)
from cStringIO import StringIO
import time

import gevent
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory

from ..async import AsyncResult
from ..constants import (
  ChannelState,
  SinkProperties,
  SinkRole
)
from ..message import (
  ClientError,
  Deadline,
  MethodCallMessage,
  MethodReturnMessage,
  TimeoutError
)
from ..sink import (
  SinkProvider,
  SinkProviderBase,
  ClientMessageSink,
)
from ..thrift.socket import TSocket
from ..varz import (
  Counter,
  AggregateTimer,
  AverageTimer,
  SourceType,
  VarzBase,
  VarzSocketWrapper
)
from .serializer import MessageSerializer

class ChannelConcurrencyError(Exception): pass

class NoopTimeout(object):
  def start(self): pass
  def cancel(self): pass

class SocketTransportSink(ClientMessageSink):
  """A sink to transport thrift method calls over a socket.

  This sink does not support processing multiple messages in parallel and will
  raise an exception if it detects it is about to.
  """
  class Varz(VarzBase):
    """
    messages_sent - The number of messages sent over this sink.
    messages_recv - The number of messages received over this sink.
    send_time - The aggregate amount of time spent sending data.
    recv_time - The aggregate amount of time spend receiving data.
    send_latency - The average amount of time taken to send a message.
    recv_latency - The average amount of time taken to receive a message
                   (once a response has reached the client).
    transport_latency - The average amount of time taken to perform a full
                        method call transaction (send data, wait for response,
                        read response).
    """

    _VARZ_BASE_NAME = 'scales.thrift.SocketTransportSink'
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'messages_sent': Counter,
      'messages_recv': Counter,
      'send_time': AggregateTimer,
      'recv_time': AggregateTimer,
      'send_latency': AverageTimer,
      'recv_latency': AverageTimer,
      'transport_latency': AverageTimer
    }

  def __init__(self, socket, source):
    super(SocketTransportSink, self).__init__()
    self._socket = socket
    self._state = ChannelState.Idle
    socket_source = '%s:%d' % (self._socket.host, self._socket.port)
    self._varz = self.Varz((source, socket_source))
    self._processing = None
    self._open_result = None

  def Open(self):
    if not self._open_result:
      self._open_result = AsyncResult()
      self._open_result.SafeLink(self._OpenImpl)
    return self._open_result

  def _OpenImpl(self):
    try:
      self._socket.open()
      self._state = ChannelState.Open
    except Exception:
      self._Fault('Open failed')
      raise

  def Close(self):
    self._state = ChannelState.Closed
    self._socket.close()
    self._open_result = None
    if self._processing:
      p, self._processing = self._processing, None
      p.kill(block=False)

  @property
  def state(self):
    if self._socket.isOpen():
      return ChannelState.Open
    else:
      return self._state

  def _Fault(self, reason):
    """Shutdown the sink and signal.

    Args:
      reason - The reason the shutdown occurred.  May be an exception or string.
    """
    if self.state == ChannelState.Closed:
      return

    self.Close()
    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    self._on_faulted.Set(reason)

  def _AsyncProcessTransaction(self, data, sink_stack, deadline):
    """Process a method call/response transaction.
    When complete the response is dispatched to the sink_stack.

    Args:
      data - The data to send.
      sink_stack - The sink_stack for this method call.
      deadline - The absolute deadline the transaction must complete by.
    """
    with self._varz.transport_latency.Measure():
      gtimeout = None
      try:
        if deadline:
          timeout = deadline - time.time()
          if timeout < 0:
            raise gevent.Timeout()
          gtimeout = gevent.Timeout.start_new(timeout)
        else:
          gtimeout = NoopTimeout()

        with self._varz.send_time.Measure():
          with self._varz.send_latency.Measure():
            self._socket.write(data)
        self._varz.messages_sent()

        sz, = unpack('!i', str(self._socket.readAll(4)))
        with self._varz.recv_time.Measure():
          with self._varz.recv_latency.Measure():
            buf = StringIO(self._socket.readAll(sz))
        self._varz.messages_recv()

        gtimeout.cancel()
        self._processing = None
        gevent.spawn(self._ProcessReply, buf, sink_stack)
      except gevent.Timeout: # pylint: disable=E0712
        err = TimeoutError()
        self._socket.close()
        self._socket.open()
        self._processing = None
        sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=err))
      except Exception as ex:
        if gtimeout:
          gtimeout.cancel()
        self._Fault(ex)
        self._processing = None
        sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=ex))

  @staticmethod
  def _ProcessReply(buf, sink_stack):
    try:
      buf.seek(0)
      sink_stack.AsyncProcessResponseStream(buf)
    except Exception as ex:
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=ex))

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self._processing is not None:
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(
        error=ChannelConcurrencyError(
          'Concurrency violation in AsyncProcessRequest')))
      return

    payload = stream.getvalue()
    sz = pack('!i', len(payload))
    deadline = msg.properties.get(Deadline.KEY)
    self._processing = gevent.spawn(self._AsyncProcessTransaction, sz + payload, sink_stack, deadline)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass


class SocketTransportSinkProvider(SinkProviderBase):
  SINK_CLS = SocketTransportSink
  Role = SinkRole.Transport

  def CreateSink(self, properties):
    server = properties[SinkProperties.Endpoint]
    service = properties[SinkProperties.Label]
    sock = TSocket.TSocket(server.host, server.port)
    healthy_sock = VarzSocketWrapper(sock, service)
    sink = self.SINK_CLS(healthy_sock, service)
    return sink

  @property
  def sink_class(self):
    return self.SINK_CLS

SocketTransportSink.Builder = SocketTransportSinkProvider


class ThriftSerializerSink(ClientMessageSink):
  """A sink for serializing thrift method calls."""

  def __init__(self, next_provider, sink_properties, global_properties):
    super(ThriftSerializerSink, self).__init__()
    self._serializer = MessageSerializer(
        global_properties[SinkProperties.ServiceInterface],
        sink_properties.protocol_factory)
    self.next_sink = next_provider.CreateSink(global_properties)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    buf = StringIO()
    headers = {}

    if not isinstance(msg, MethodCallMessage):
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(
          error=ClientError('Invalid message type.')))
      return
    try:
      self._serializer.SerializeThriftCall(msg, buf)
    except Exception as ex:
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=ex))
      return

    sink_stack.Push(self)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, buf, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    if msg:
      # No need to deserialize, it already is.
      sink_stack.AsyncProcessResponseMessage(msg)
    else:
      try:
        msg = self._serializer.DeserializeThriftCall(stream)
      except Exception as ex:
        msg = MethodReturnMessage(error=ex)
      sink_stack.AsyncProcessResponseMessage(msg)

ThriftSerializerSink.Builder = SinkProvider(
  ThriftSerializerSink,
  SinkRole.Formatter,
  protocol_factory=TBinaryProtocolAcceleratedFactory())
