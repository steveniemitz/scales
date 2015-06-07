from .sink import (
    ThriftMuxMessageSerializerSinkProvider,
    SocketTransportSinkProvider,
)

from ..builder import BaseBuilder
from ..channel_resurrector import ResurrectorChannelSinkProvider
from ..loadbalancer.aperture import ApertureBalancerChannelSinkProvider
from ..pool import SingletonPoolChannelSinkProvider


class ThriftMux(BaseBuilder):
  """A builder class for building clients to ThriftMux services."""
  class SinkProvider(BaseBuilder.SinkProvider):
    _PROVIDERS = [
      ThriftMuxMessageSerializerSinkProvider,
      ApertureBalancerChannelSinkProvider,
      ResurrectorChannelSinkProvider,
      SingletonPoolChannelSinkProvider,
      SocketTransportSinkProvider
    ]