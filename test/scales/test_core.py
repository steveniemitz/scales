import unittest

from scales.core import ScalesUriParser, ClientProxyBuilder
from scales.loadbalancer.serverset import (
  StaticServerSetProvider,
  ZooKeeperServerSetProvider)

Server = ScalesUriParser.Server
Endpoint = ScalesUriParser.Endpoint

class CoreCase(unittest.TestCase):
  def testScalesUriParser_tcp(self):
    parser = ScalesUriParser()
    ret = parser.Parse('tcp://localhost:8080,localhost:8081')
    self.assertIsInstance(ret, StaticServerSetProvider)
    self.assertEqual(ret.GetServers(), [
      Server(Endpoint('localhost', 8080)),
      Server(Endpoint('localhost', 8081))])

  def testScalesUriParser_zk(self):
    parser = ScalesUriParser()
    ret = parser.Parse('zk://zk1.zk.com:2181/test/path')
    self.assertIsInstance(ret, ZooKeeperServerSetProvider)
    self.assertEqual(ret._zk_path, 'test/path')
    self.assertEqual(list(ret._zk_client.hosts), [('zk1.zk.com', 2181)])

  def testClientProxyBuilder(self):
    class MockDispatcher(object):
      def Close(self):
        pass

    class TestIface(object):
      def M1(self): pass
      def M2(self): pass

    proxy_cls = ClientProxyBuilder.CreateServiceClient(TestIface)
    methods = set(dir(proxy_cls))
    self.assertIn('M1', methods)
    self.assertIn('M1_async', methods)
    self.assertIn('M2', methods)
    self.assertIn('M2_async', methods)
    self.assertIn('DispatcherOpen', methods)
    self.assertIn('DispatcherClose', methods)
    self.assertIsInstance(proxy_cls(MockDispatcher()), TestIface)


if __name__ == '__main__':
  unittest.main()
