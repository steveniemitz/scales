import logging

from ..async import AsyncResult, NamedGreenlet
from ..constants import ConnectionRole, SinkProperties, MessageProperties
from ..message import MethodReturnMessage
from ..mux.sink import MuxSocketTransportSink
from ..sink import ServerMessageSink, ServerChannelSink, SinkProvider, ClientMessageSinkStack
from ..scales_socket import ScalesSocket
from .sink import ThriftMuxMessageSerializerSink
from .protocol import MessageType
from .serializer import MessageSerializer

ROOT_LOG = logging.getLogger('scales.thriftmux')

class ServerMuxSocketTransportSink(MuxSocketTransportSink):
  def __init__(self, socket, service, sink_stack):
    super(ServerMuxSocketTransportSink, self).__init__(socket, service, ConnectionRole.Server)
    self.next_sink = sink_stack
    self._log = ROOT_LOG.getChild('ServerTransportSink')

  def _CheckInitialConnection(self):
    pass

  def _OnTimeout(self, tag):
    pass

  def _BuildHeader(self, tag, msg_type, data_len):
    pass

  def _ProcessRecv(self, stream):
    msg_type, tag = ThriftMuxMessageSerializerSink.ReadHeader(stream)
    if msg_type == MessageType.Tping:
      self._Send(MessageSerializer.BuildHeader(tag, MessageType.Rping, 0), {})
    elif msg_type == MessageType.Tdispatch:
      self._DispatchMessage(stream)
    else:
      self._log.error("Invalid message type recieved: %s", msg_type)

  def _DispatchMessage(self, stream):
    stream.seek(0)
    stack = ClientMessageSinkStack()
    stack.Push(self, tag)
    self.next_sink.AsyncProcessRequest(stack, None, stream, {})

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    body = stream.getvalue()
    header = MessageSerializer.BuildHeader(context, MessageType.Rdispatch, len(body))
    self._Send(header + body, {})

class ThriftMuxServerSocketSink(ServerChannelSink):
  SINK_LOG = ROOT_LOG.getChild('ServerTransportSink')

  def __init__(self, socket, service, next_provider):
    super(ThriftMuxServerSocketSink, self).__init__()
    self._socket = socket
    self._acceptor = None
    self._service = service
    self._socket_source = "%s:%s" % (socket.host, socket.port)
    self._clients = {}
    self._log = self.SINK_LOG.getChild('[%s.%s:%d]' % (
        service, socket.host, socket.port))
    self._next_sink = next_provider.CreateSink({
      SinkProperties.ServiceInterface: service,
      SinkProperties.Label: 'server'
    })

  def _AcceptLoop(self):
    while True:
      try:
        client_socket, addr = self._socket.accept()
        self._log.info("Accepted connection from client %s", str(addr))
        client_socket = ScalesSocket.fromAccept(client_socket, addr)
        client = ServerMuxSocketTransportSink(client_socket, self._service, self._next_sink)
        self._clients[addr] = client
        client.Open()
      except:
        self._log.exception("Error calling accept()")

  def Open(self):
    if not self._acceptor:
      self._acceptor = NamedGreenlet(self._AcceptLoop)
      self._acceptor.name = 'Scales AcceptLoop for %s [%s]' % (self._service, self._socket_source)
      self._socket.listen(1000)
      self._log.info("Listening on %s", self._socket_source)
      self._acceptor.start()
    return AsyncResult.Complete()

class ServerCallBuilderSink(ServerMessageSink):
  def __init__(self, next_provider, sink_properties, global_properties):
    super(ServerCallBuilderSink, self).__init__()
    self._handler = sink_properties.handler

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    fn = getattr(self._handler, msg.method)
    if not callable(fn):
      ret_msg = MethodReturnMessage(error=Exception("Unable to find callable for method %s" % msg.method))
      sink_stack.AsyncProcessResponseMessage(ret_msg)
      return

    ar = AsyncResult()
    ar.SafeLink(lambda : fn(*msg.args, **msg.kwargs))
    ar.ContinueWith(
        lambda _ar: self._ProcessMethodResponse(_ar, sink_stack),
        on_hub=True
    )

  def _ProcessMethodResponse(self, ar, sink_stack):
    if not ar.successful:
      msg = MethodReturnMessage(error=ar.exception)
    else:
      msg = MethodReturnMessage(return_value=ar.value)
    sink_stack.AsyncProcessResponseMessage(msg)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass

ServerCallBuilderSink.Builder = SinkProvider(
    ServerCallBuilderSink,
    handler=None)
