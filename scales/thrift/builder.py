from thrift.transport import TTransport
from thrift.transport import TSocket

from .sink import (
  ThriftFormatterSink,
  SocketTransportSink,
)
from ..sink import (MessageSinkStackBuilder, TransportSinkStackBuilder)
from ..builder import BaseBuilder
from ..varzsocketwrapper import VarzSocketWrapper


class Thrift(BaseBuilder):
  """A builder for Thrift service clients."""
  class TransportSinkStackBuilder(TransportSinkStackBuilder):
    def AreTransportsSharable(self):
      return False

    def CreateSinkStack(self, server, pool_name):
      sock = TSocket.TSocket(server.host, server.port)
      healthy_sock = VarzSocketWrapper(sock, pool_name)
      sink = SocketTransportSink(healthy_sock, pool_name)
      return sink

    def IsConnectionFault(self, e):
      return isinstance(e,  TTransport.TTransportException)

  class MessageSinkStackBuilder(MessageSinkStackBuilder):
    def CreateSinkStack(self, name):
      return ThriftFormatterSink(name)
