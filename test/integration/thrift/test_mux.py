import unittest

from integration.thrift.test_thrift import ThriftTestCase
from scales.thriftmux import ThriftMux

class ThriftMuxTestCase(ThriftTestCase):
  BUILDER = ThriftMux

if __name__ == '__main__':
  unittest.main()
