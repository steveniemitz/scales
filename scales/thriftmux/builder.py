from .sink import (
    ThriftMuxMessageSerializerSinkProvider,
    SocketTransportSinkProvider,
)

from ..builder import BaseBuilder
from ..loadbalancer import ApertureBalancerSinkProvider
from ..resurrector import ResurrectorSinkProvider


class ThriftMux(BaseBuilder):
  """A builder class for building clients to ThriftMux services."""
  class SinkProviderProvider(BaseBuilder.SinkProviderProvider):
    _PROVIDERS = [
      ThriftMuxMessageSerializerSinkProvider,
      ApertureBalancerSinkProvider,
      ResurrectorSinkProvider,
      SocketTransportSinkProvider
    ]
