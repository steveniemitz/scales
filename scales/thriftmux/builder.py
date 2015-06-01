from .sink import (
    ThriftMuxMessageSerializerSink,
    SocketTransportSinkProvider,
)

from ..builder import BaseBuilder
from ..channel_resurrector import ResurrectorChannelSinkProvider
from ..loadbalancer import ApertureBalancerChannelSink
from ..pool import SingletonPoolChannelSinkProvider
from ..sink import MessageSinkStackBuilder


class ThriftMux(BaseBuilder):
  """A builder class for building clients to ThriftMux services."""
  class MessageSinkStackBuilder(MessageSinkStackBuilder):
    def CreateSinkStack(self, builder):
      name = builder.name

      transport_provider = SocketTransportSinkProvider()
      pool_provider = SingletonPoolChannelSinkProvider()
      pool_provider.next_provider = transport_provider

      resurrector = ResurrectorChannelSinkProvider()
      resurrector.next_provider = pool_provider

      balancer = ApertureBalancerChannelSink(resurrector, name, builder.server_set_provider)

      serializer = ThriftMuxMessageSerializerSink(name)
      serializer.next_sink = balancer
      return serializer
