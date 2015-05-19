from weakref import (
  WeakValueDictionary
)

from thrift.transport import TTransport
from thrift.transport import TSocket

from .sink import (
    ThrfitMuxMessageSerializerSink,
    SocketTransportSink,
    TimeoutSink
)
from ..builder import BaseBuilder
from ..sink import (
  TransportSinkStackBuilder,
  MessageSinkStackBuilder
)
from ..varzsocketwrapper import VarzSocketWrapper

class ThriftMux(BaseBuilder):
  """A builder class for building clients to ThriftMux services."""
  class MessageSinkStackBuilder(MessageSinkStackBuilder):
    def CreateSinkStack(self, source):
      sink = TimeoutSink(source)
      sink.next_sink = ThrfitMuxMessageSerializerSink(source)
      return sink

  class TransportSinkStackBuilder(TransportSinkStackBuilder):
    _MUXERS = WeakValueDictionary()

    def AreTransportsSharable(self):
      return True

    def _CreateSocket(self, host, port):
      return TSocket.TSocket(host, port)

    def CreateSinkStack(self, server, pool_name):
      key = (server, pool_name)
      if key in self._MUXERS:
        sink = self._MUXERS[key]
      else:
        sock = self._CreateSocket(server.host, server.port)
        healthy_sock = VarzSocketWrapper(sock, pool_name)
        sink = SocketTransportSink(healthy_sock, pool_name)
        self._MUXERS[key] = sink
      return sink

    def IsConnectionFault(self, e):
      return isinstance(e,  TTransport.TTransportException)
