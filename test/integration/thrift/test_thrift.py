import unittest

import gevent
from scales.thrift import Thrift

from integration.thrift.gen_py.example_rpc_service import (
  ExampleService,
  ttypes
)

class ThriftTestCase(unittest.TestCase):
  BUILDER = Thrift

  def setUp(self):
    self.client = self.BUILDER.NewClient(ExampleService.Iface, 'tcp://localhost:8080')

  def _send_request(self):
    try:
      ret = self.client.passMessage(ttypes.Message('hi!'))
      return ret.content == 'hi! server!'
    except:
      raise
      return False

  def test(self):
    ret = self._send_request()
    self.assertTrue(ret)

  def test_concurrency(self):
    num_threads = 3
    num_iterations = 1000
    def _test():
      for _ in xrange(num_iterations):
        self.assertTrue(self._send_request())

    greenlets = []
    for _ in xrange(num_threads):
      greenlets.append(gevent.spawn(_test))

    [g.join() for g in greenlets]

if __name__ == '__main__':
  unittest.main()
