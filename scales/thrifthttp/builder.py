from thrift.protocol.TJSONProtocol import TJSONProtocolFactory

from .sink import (
  ThriftHttpTransportSink
)
from ..core import Scales
from ..loadbalancer.aperture import ApertureBalancerSink
from ..pool import SingletonPoolSink
from ..thrift.sink import ThriftSerializerSink

class ThriftHttp(object):
  @staticmethod
  def NewBuilder(Iface, url_suffix):
    return Scales.NewBuilder(Iface) \
      .WithSink(ThriftSerializerSink.Builder(protocol_factory=TJSONProtocolFactory())) \
      .WithSink(ApertureBalancerSink.Builder()) \
      .WithSink(SingletonPoolSink.Builder()) \
      .WithSink(ThriftHttpTransportSink.Builder(url=url_suffix))

  @staticmethod
  def NewClient(Iface, uri, timeout=60, url_suffix=''):
    return ThriftHttp.NewBuilder(Iface, url_suffix) \
      .SetUri(uri) \
      .SetTimeout(timeout) \
      .Build()
