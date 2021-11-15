from .sink import (
    ClientIdInterceptorSink,
    ThriftMuxMessageSerializerSink,
    SocketTransportSink,
)
from ..core import Scales
from ..loadbalancer import ApertureBalancerSink
from ..resurrector import ResurrectorSink


class ThriftMux(object):
  @staticmethod
  def NewBuilder(Iface, client_id=None):
    builder = Scales.NewBuilder(Iface)
    if client_id:
      builder = builder.WithSink(ClientIdInterceptorSink.Builder(client_id=client_id))
    return builder.WithSink(ThriftMuxMessageSerializerSink.Builder()) \
      .WithSink(ApertureBalancerSink.Builder()) \
      .WithSink(ResurrectorSink.Builder()) \
      .WithSink(SocketTransportSink.Builder())

  @staticmethod
  def NewClient(Iface, uri, timeout=10, client_id=None, service_identifier=None, opportunistic_tls=None):
    return ThriftMux.NewBuilder(Iface, client_id=client_id) \
      .SetUri(uri) \
      .SetTimeout(timeout) \
      .SetServiceIdentifier(service_identifier) \
      .SetOpportunisticTls(opportunistic_tls) \
      .Build()
