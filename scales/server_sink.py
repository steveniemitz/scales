from abc import abstractmethod
import logging

import gevent

from .async import AsyncResult, NamedGreenlet
from .message import MethodReturnMessage
from .scales_socket import ScalesSocket
from .sink import (
  ClientMessageSink,
  Sink,
  SinkProperties,
  SinkProvider,
  SocketTransportSinkProvider,
)

ROOT_LOG = logging.getLogger('scales.server_sink')

class ServerMessageSink(ClientMessageSink):
  pass

class ServerChannelSink(Sink):
  Builder = None

  def __init__(self):
    super(ServerChannelSink, self).__init__()
    self.close_event = AsyncResult()

  def Wait(self):
    self.close_event.wait()

  @abstractmethod
  def Open(self):
    pass

  @abstractmethod
  def Close(self):
    pass

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

  @staticmethod
  def _ProcessMethodResponse(ar, sink_stack):
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


class TcpServerChannel(ServerChannelSink):
  SINK_LOG = ROOT_LOG.getChild('TcpServerChannel')

  def __init__(self, socket, service, next_provider, sink_properties, global_properties):
    super(TcpServerChannel, self).__init__()
    self._socket = socket
    self._acceptor = None
    self._service = service
    self._socket_source = "%s:%s" % (socket.host, socket.port)
    self._clients = {}
    self._next_provider = next_provider
    self._global_properties = global_properties
    self._log = self.SINK_LOG.getChild('[%s.%s:%d]' % (
      service, socket.host, socket.port))

  def _AcceptLoop(self):
    while True:
      try:
        client_socket, addr = self._socket.accept()
        self._log.info("Accepted connection from client %s", str(addr))

        # Create a scales socket
        client_socket = ScalesSocket.fromAccept(client_socket, addr)

        # Init properties to launch the new sink
        new_props = self._global_properties.copy()
        new_props[SinkProperties.Socket] = client_socket
        client = self._next_provider.CreateSink(new_props)

        # Set up the client fault handler to correctly remove the client
        self._clients[addr] = client
        client.on_faulted.Subscribe(lambda _: self._clients.pop(addr, None), True)
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
      self.close_event = AsyncResult()
    return AsyncResult.Complete()

  def Close(self):
    self.close_event.set(None)

TcpServerChannel.Builder = SocketTransportSinkProvider(TcpServerChannel)
