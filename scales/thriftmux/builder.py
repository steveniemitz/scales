from .sink import (
  ThriftMuxMessageSerializerSink,
  SocketTransportSink,
)
from .server_sink import (
  ThriftMuxServerSocketTransportSink,
  ThriftMuxServerMessageSerializerSink
)
from ..core import Scales
from ..loadbalancer import ApertureBalancerSink
from ..resurrector import ResurrectorSink
from ..server_sink import TcpServerChannel

class ThriftMux(object):
  @staticmethod
  def NewBuilder(Iface):
    return Scales.NewClientBuilder(Iface) \
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

  @staticmethod
  def NewServerBuilder(Iface, handler, endpoint):
    return Scales.NewServerBuilder(Iface, handler) \
      .WithSink(TcpServerChannel.Builder()) \
      .WithSink(ThriftMuxServerSocketTransportSink.Builder()) \
      .WithSink(ThriftMuxServerMessageSerializerSink.Builder()) \
      .SetLocalEndpoint(endpoint) \
      .Build()