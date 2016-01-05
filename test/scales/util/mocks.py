import gevent

from scales.async import AsyncResult
from scales.constants import ChannelState, SinkProperties
from scales.core import ScalesUriParser
from scales.message import (MethodReturnMessage, FailedFastError)
from scales.loadbalancer.serverset import ServerSetProvider
from scales.sink import (ClientMessageSink, ClientMessageSinkStack, SinkProviderBase)
from scales.varz import VarzSocketWrapper
from scales.scales_socket import ScalesSocket

class MockSinkStack(ClientMessageSinkStack):
  def __init__(self):
    self.processed_response = False
    self.return_message = None
    self.return_stream = None
    super(ClientMessageSinkStack, self).__init__()

  def AsyncProcessResponse(self, stream, msg):
    self.processed_response = True
    self.return_message = msg
    self.return_stream = stream
    super(MockSinkStack, self).AsyncProcessResponse(stream, msg)


class MockSink(ClientMessageSink):
  def __init__(self, properties):
    super(MockSink, self).__init__()
    self._state = ChannelState.Idle
    self.ProcessRequest = None
    self.ProcessResponse = None
    self._open_delay = properties.get('open_delay', 0)
    self._num_failures = properties.get('num_failures', [0])
    self._properties = properties
    self._open_result = None
    self.endpoint = properties[SinkProperties.Endpoint]

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self.ProcessRequest:
      self.ProcessRequest(sink_stack, msg, stream, headers)
    else:
      if self.is_closed:
        sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=FailedFastError()))

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    if self.ProcessResponse:
      self.ProcessResponse(sink_stack, context, stream, msg)

  @property
  def state(self):
    return self._state

  @state.setter
  def state(self, value):
    self._state = value

  def Open(self):
    if not self._open_result:
      def open_impl():
        if self._num_failures[0] > 0:
          self._num_failures[0] -= 1
          self._state = ChannelState.Closed
          raise Exception("Error opening socket")
        gevent.sleep(self._open_delay)
        self._state = ChannelState.Open
      self._open_result = AsyncResult()
      self._open_result.SafeLink(open_impl)
    return self._open_result

  def Close(self):
    self._state = ChannelState.Closed
    self._open_result = None

  def Fault(self):
    self._state = ChannelState.Closed
    self.on_faulted.Set()


class MockSinkProvider(SinkProviderBase):
  def __init__(self):
    super(MockSinkProvider, self).__init__()
    self.ProcessRequest = None
    self.ProcessResponse = None
    self.sinks_created = []

  def CreateSink(self, properties):
    ms = MockSink(properties)
    ms.ProcessRequest = self.ProcessRequest
    ms.ProcessResponse = self.ProcessResponse
    self.sinks_created.append(ms)
    return ms

  @property
  def sink_class(self):
    return MockSink


class MockServerSetProvider(ServerSetProvider):
  def __init__(self):
    self._servers = set()
    self._on_leave = None
    self._on_join = None

  def Initialize(self, on_join, on_leave):
    self._on_join = on_join
    self._on_leave = on_leave

  def Close(self):
    pass

  def AddServer(self, host, port):
    ep = ScalesUriParser.Endpoint(host, port)
    server = ScalesUriParser.Server(ep)
    self._servers.add(server)
    if self._on_join:
      self._on_join(server)

  def RemoveServer(self, host, port):
    server = next(n for n in self._servers if n.service_endpoint.host == host and n.service_endpoint.port == port)
    self._servers.discard(server)
    if self._on_leave:
      self._on_leave(server)

  def GetServers(self):
    return [s for s in self._servers]

  def RemoveAllServers(self):
    while self._servers:
      s = self._servers.pop()
      if self._on_leave:
        self._on_leave(s)

def noop(*args, **kwargs): pass

class MockSocket(ScalesSocket):
  def __init__(self, host, port, open=None, close=None, read=None, write=None):
    super(MockSocket, self).__init__(host, port)
    self.host = host
    self.port = port
    self._open = open or noop
    self._close = close or noop
    self._read = read or noop
    self._write = write or noop
    self._is_open = False

  def open(self):
    self._open()
    self._is_open = True

  def close(self):
    self._close()
    self._is_open = False

  def isOpen(self):
    return self._is_open

  def read(self, sz):
    return self._read(sz)

  def write(self, buff):
    return self._write(buff)
