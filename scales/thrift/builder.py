from thrift.transport import TSocket

from ..channel_resurrector import ResurrectorChannelSinkProvider
from ..loadbalancer import ApertureBalancer
from ..pool import WatermarkPoolChannelSinkProvider


from .sink import (
  ThriftFormatterSink,
  SocketTransportSink,
)
from ..sink import (MessageSinkStackBuilder, TransportSinkStackBuilder)
from ..builder import BaseBuilder
from ..varz import VarzSocketWrapper


class Thrift(BaseBuilder):
  """A builder for Thrift service clients."""
  class TransportSinkStackBuilder(TransportSinkStackBuilder):
    def CreateSink(self, server, pool_name):
      sock = TSocket.TSocket(server.host, server.port)
      healthy_sock = VarzSocketWrapper(sock, pool_name)
      sink = SocketTransportSink(healthy_sock, pool_name)
      return sink

  class MessageSinkStackBuilder(MessageSinkStackBuilder):
    def CreateSinkStack(self, builder):
      name = builder.name
      formatter = ThriftFormatterSink(name)
      pool_provider = WatermarkPoolChannelSinkProvider(Thrift.TransportSinkStackBuilder())
      resurrector = ResurrectorChannelSinkProvider(pool_provider)
      balancer = ApertureBalancer(resurrector, name, builder.server_set_provider)
      formatter.next_sink = balancer
      return formatter
