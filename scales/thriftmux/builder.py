from .sink import (
    ThriftMuxMessageSerializerSink,
    SocketTransportSink,
)
from ..core import Scales
from ..loadbalancer import ApertureBalancerSink
from ..resurrector import ResurrectorSink


class ThriftMux(object):
  @staticmethod
  def NewBuilder(Iface):
    return Scales.NewBuilder(Iface) \
      .WithSink(ThriftMuxMessageSerializerSink.Builder()) \
      .WithSink(ApertureBalancerSink.Builder()) \
      .WithSink(ResurrectorSink.Builder()) \
      .WithSink(SocketTransportSink.Builder())

  @staticmethod
  def NewClient(Iface, uri, timeout=10):
    return ThriftMux.NewBuilder(Iface) \
      .SetUri(uri) \
      .SetTimeout(timeout) \
      .Build()
