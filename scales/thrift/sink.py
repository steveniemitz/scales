from struct import (pack, unpack)
from cStringIO import StringIO
import time

import gevent
from thrift.transport import TTransport

from ..constants import ChannelState
from ..message import (
  ClientError,
  Deadline,
  MethodCallMessage,
  MethodReturnMessage,
  TimeoutError
)
from ..sink import (
  ChannelSinkProviderBase,
  ClientChannelTransportSink,
  ClientChannelSinkStack,
  ClientFormatterSink,
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
from .formatter import MessageSerializer

class NoopTimeout(object):
  def start(self): pass
  def cancel(self): pass

class SocketTransportSink(ClientChannelTransportSink):
  class Varz(VarzBase):
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

  def Open(self):
    try:
      self._socket.open()
    except TTransport.TTransportException:
      self._Fault('Open failed')
      raise
    self._state = ChannelState.Open

  def Close(self):
    self._state = ChannelState.Closed
    self._socket.close()
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

        sz, = unpack('!i', self._socket.readAll(4))
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
        sink_stack.AsyncProcessResponse(None, MethodReturnMessage(error=err))
      except Exception as ex:
        if gtimeout:
          gtimeout.cancel()
        self._Fault(ex)
        self._processing = None
        sink_stack.AsyncProcessResponse(None, MethodReturnMessage(error=ex))

  @staticmethod
  def _ProcessReply(buf, sink_stack):
    try:
      buf.seek(0)
      sink_stack.AsyncProcessResponse(buf)
    except Exception as ex:
      sink_stack.AsyncProcessResponse(None, MethodReturnMessage(error=ex))

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self._processing is not None:
      sink_stack.AsyncProcessResponse(None, MethodReturnMessage(
        error=Exception('Concurrency violation in AsyncProcessRequest')))
      return

    payload = stream.getvalue()
    sz = pack('!i', len(payload))
    deadline = msg.properties.get(Deadline.KEY)
    self._processing = gevent.spawn(self._AsyncProcessTransaction, sz + payload, sink_stack, deadline)



class ThriftFormatterSink(ClientFormatterSink):
  def __init__(self, source):
    super(ThriftFormatterSink, self).__init__()

  def AsyncProcessMessage(self, msg, reply_sink):
    buf = StringIO()
    headers = {}

    if not isinstance(msg, MethodCallMessage):
      reply_sink.ProcessReturnMessage(MethodReturnMessage(
          error=ClientError('Invalid message type.')))
    try:
      MessageSerializer.SerializeThriftCall(msg, buf)
      ctx = msg.service, msg.method
    except Exception as ex:
      reply_sink.ProcessReturnMessage(MethodReturnMessage(error=ex))
      return

    sink_stack = ClientChannelSinkStack(reply_sink)
    sink_stack.Push(self, ctx)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, buf, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    if msg:
      sink_stack.DispatchReplyMessage(msg)
    else:
      try:
        msg = MessageSerializer.DeserializeThriftCall(stream, context)
      except Exception as ex:
        msg = MethodReturnMessage(error=ex)
      sink_stack.DispatchReplyMessage(msg)

  def Open(self, force=False):
    pass

  def Close(self):
    pass

  @property
  def state(self):
    pass


class SocketTransportSinkProvider(ChannelSinkProviderBase):
  def CreateSink(self, server, pool_name, properties):
    sock = TSocket.TSocket(server.host, server.port)
    healthy_sock = VarzSocketWrapper(sock, pool_name)
    sink = SocketTransportSink(healthy_sock, pool_name)
    return sink
