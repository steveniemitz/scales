from abc import abstractmethod
import logging

import time
from gevent import GreenletExit

from .async import AsyncResult, NamedGreenlet
from .constants import ChannelState
from .message import Deadline, MethodReturnMessage, TimeoutError
from .scales_socket import ScalesSocket
from .sink import (
  ClientMessageSink,
  ClientTimeoutSink,
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


class ServerTimeoutSink(ClientTimeoutSink):
  def _TimeoutHelper(self, evt, sink_stack):
    if evt:
      evt.Set(True)
    self._varz.timeouts()


class ServerCallBuilderSink(ServerMessageSink):
  def __init__(self, next_provider, sink_properties, global_properties):
    super(ServerCallBuilderSink, self).__init__()
    if next_provider:
      raise Exception("ServerCallBuilderSink must be the last sink in the chain.")
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
    self._state = ChannelState.Idle
    self._log = self.SINK_LOG.getChild('[%s.%s:%d]' % (
      service, socket.host, socket.port))

  def state(self):
    return self._state

  def _AcceptLoop(self):
    while True:
      client_socket, addr = None, None
      try:
        client_socket, addr = self._socket.accept()
        # Create a scales socket
        client_socket = ScalesSocket.fromAccept(client_socket, addr)
        self._log.info("Accepted connection from client %s", str(addr))
      except GreenletExit:
        return
      except:
        self._log.exception("Error calling accept, dropping connection.")
        continue

      try:
        # Init properties to launch the new sink
        new_props = self._global_properties.copy()
        new_props[SinkProperties.Socket] = client_socket
        client = self._next_provider.CreateSink(new_props)

        # Set up the client fault handler to correctly remove the client
        self._clients[addr] = client
        client.on_faulted.Subscribe(lambda _: self._CloseClient(addr), True)
        client.Open()
      except GreenletExit:
        return
      except:
        self._log.exception("Error setting up sink chain for new client %s", str(addr))
        client_socket.close()

  def _CloseClient(self, addr):
    client = self._clients.pop(addr, None)
    if not client:
      self._log.warn("Unable to find client for address %s.", addr)
      return

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
    if self.state != ChannelState.Open:
      return

    self._state = ChannelState.Closed
    if self._acceptor:
      self._acceptor.kill(block=False)

    for addr, c in self._clients.items():
      c.Close()

    self._acceptor = None
    self._clients = {}
    self.close_event.set(None)

TcpServerChannel.Builder = SocketTransportSinkProvider(TcpServerChannel)
