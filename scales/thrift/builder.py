from ..constants import SinkProperties
from ..channel_resurrector import ResurrectorChannelSinkProvider
from ..loadbalancer.aperture import ApertureBalancerChannelSinkProvider
from ..pool import WatermarkPoolChannelSinkProvider
from .sink import (
  SocketTransportSinkProvider,
  ThriftFormatterSinkProvider,
)

from ..builder import BaseBuilder


class Thrift(BaseBuilder):
  """A builder for Thrift service clients."""
  class SinkProvider(BaseBuilder.SinkProvider):
    _PROVIDERS = [
      ThriftFormatterSinkProvider,
      ApertureBalancerChannelSinkProvider,
      ResurrectorChannelSinkProvider,
      WatermarkPoolChannelSinkProvider,
      SocketTransportSinkProvider
    ]