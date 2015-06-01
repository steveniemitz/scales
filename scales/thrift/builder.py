
from ..channel_resurrector import ResurrectorChannelSinkProvider
from ..loadbalancer import ApertureBalancerChannelSink
from ..pool import WatermarkPoolChannelSinkProvider
from .sink import (
  SocketTransportSinkProvider,
  ThriftFormatterSink,
)
from ..sink import MessageSinkStackBuilder
from ..builder import BaseBuilder


class Thrift(BaseBuilder):
  """A builder for Thrift service clients."""
  class MessageSinkStackBuilder(MessageSinkStackBuilder):
    def CreateSinkStack(self, builder):
      name = builder.name
      formatter = ThriftFormatterSink(name)
      pool_provider = WatermarkPoolChannelSinkProvider()
      pool_provider.next_provider = SocketTransportSinkProvider()

      resurrector = ResurrectorChannelSinkProvider()
      resurrector.next_provider = pool_provider
      balancer = ApertureBalancerChannelSink(resurrector, name, builder.server_set_provider)
      formatter.next_sink = balancer
      return formatter
