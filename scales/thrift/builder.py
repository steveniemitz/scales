from ..resurrector import ResurrectorSinkProvider
from ..loadbalancer import ApertureBalancerSinkProvider
from ..pool import WatermarkPoolChannelSinkProvider
from .sink import (
  SocketTransportSinkProvider,
  ThriftFormatterSinkProvider,
)

from ..builder import BaseBuilder


class Thrift(BaseBuilder):
  """A builder for Thrift clients."""
  class SinkProviderProvider(BaseBuilder.SinkProviderProvider):
    _PROVIDERS = [
      ThriftFormatterSinkProvider,
      ApertureBalancerSinkProvider,
      ResurrectorSinkProvider,
      WatermarkPoolChannelSinkProvider,
      SocketTransportSinkProvider
    ]
