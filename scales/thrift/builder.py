from ..resurrector import ResurrectorSink
from ..loadbalancer import ApertureBalancerSink
from ..pool import WatermarkPoolSink
from .sink import (
  SocketTransportSink,
  ThriftSerializerSink,
)

from ..core import Scales


class Thrift(object):
  """A builder for Thrift clients."""

  @staticmethod
  def NewBuilder(Iface):
    return Scales.NewBuilder(Iface)\
      .WithSink(ThriftSerializerSink.Builder())\
      .WithSink(ApertureBalancerSink.Builder())\
      .WithSink(ResurrectorSink.Builder())\
      .WithSink(WatermarkPoolSink.Builder())\
      .WithSink(SocketTransportSink.Builder())

  @staticmethod
  def NewClient(Iface, uri, timeout=60):
    return Thrift.NewBuilder(Iface)\
      .SetUri(uri)\
      .SetTimeout(timeout)\
      .Build()
