import unittest

from integration.thrift.test_thrift import ThriftTestCase
from scales.core import Scales
from scales.loadbalancer.aperture import ApertureBalancerSink
from scales.loadbalancer.distributor.hashed import HashedDistributorSink
from scales.thriftmux.sink import (
  SocketTransportSink,
  ThriftMuxMessageSerializerSink
)

from integration.thrift.gen_py.example_rpc_service import ExampleService

class TestHasher(object):
  def __init__(self):
    self._i = 0

  def HashEndpoint(self, ep):
    self._i += 1
    return 1

  def HashMessage(self, msg):
    return 1

class HashedDistributorTestCase(ThriftTestCase):
  def setUp(self):
    self.client = Scales.NewBuilder(ExampleService.Iface) \
      .WithSink(ThriftMuxMessageSerializerSink.Builder()) \
      .WithSink(HashedDistributorSink.Builder(hasher=TestHasher())) \
      .WithSink(ApertureBalancerSink.Builder()) \
      .WithSink(SocketTransportSink.Builder()) \
      .SetUri('tcp://localhost:8082,127.0.0.1:8082') \
      .Build()


if __name__ == '__main__':
  unittest.main()
