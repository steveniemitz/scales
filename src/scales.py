import collections
import functools
import inspect
import types

from cStringIO import StringIO
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from thrift.transport.TTransport import TFramedTransport

from src.dispatch import MuxMessageDispatcher
from src.pool import (
  RoundRobinPoolMemberSelector,
  SingletonPool,
  StaticServerSetProvider)
from src.thealthysocket import THealthySocket

class TScalesTransport(TFramedTransport):
  def __init__(self, dispatcher_pool):
    self._dispatcher_pool = dispatcher_pool
    self._read_future = None
    TFramedTransport.__init__(self, None)

  def flush(self):
    payload = getattr(self, '_TFramedTransport__wbuf')
    setattr(self, '_TFramedTransport__wbuf', StringIO())
    self._read_future = self._dispatcher_pool.Get().SendDispatchMessage(payload)

  def readFrame(self):
    if self._read_future is None:
      raise Exception("Unexpected read!")
    else:
      data = self._read_future.get()
      setattr(self, '_TFramedTransport__rbuf', data)
      self._read_future = None

  def open(self):
    # never touch the underlying transport
    pass

  def isOpen(self):
    # always open
    return True

  def close(self):
    # never touch it
    pass


class Scales(object):
  """Factory for scales thrift services.
  """

  @staticmethod
  def _WrapClient(Client, pool):
    class WrapperPool(object):
      def __init__(self, dispatcher):
        self._dispatcher = dispatcher

      def Get(self):
        return self._dispatcher

    def Proxy(orig_method):
      @functools.wraps(orig_method)
      def _Wrap(self, *args, **kwargs):
        with pool.SafeGet() as dispatcher:
          mux = TScalesTransport(WrapperPool(dispatcher))
          prot = TBinaryProtocol.TBinaryProtocolAccelerated(mux)
          cli = Client(prot, prot)
          return orig_method(cli, *args, **kwargs)
      return types.MethodType(_Wrap, Client)

    # Find the thrift interface on the client
    iface = next(b for b in Client.__bases__ if b.__name__ == 'Iface')
    thrift_method = lambda m: inspect.ismethod(m) and not inspect.isbuiltin(m)

    # Find all methods on the thrift interface
    iface_methods = dir(iface)
    is_iface_method = lambda m: m and thrift_method(m) and m.__name__ in iface_methods

    # Then get the methods on the client that it implemented from the interface
    client_methods = { m[0]: Proxy(m[1])
                       for m in inspect.getmembers(Client, is_iface_method) }

    # Create a proxy class to intercept the thrift methods.
    proxy = type(
        '_ScalesTransparentProxy<%s>' % Client.__module__,
        (iface, object),
        client_methods)
    return proxy

  class _ServiceBuilder(object):
    Endpoint = collections.namedtuple('Endpoint', 'host port')
    Server = collections.namedtuple('Server', 'service_endpoint')

    def __init__(self, Client):
      self._client = Client
      self._name = Client.__module__
      self._uri = None
      self._zk_servers = None
      self._selector = RoundRobinPoolMemberSelector()
      self._timeout = 10
      self._initial_size_members = 0
      self._initial_size_pct = 0
      self._dispatcher_factory = None

    def setUri(self, uri):
      self._uri = uri
      return self

    def setZkServers(self, servers):
      self._zk_servers = servers
      return self

    def setPoolMemberSelector(self, selector):
      self._selector = selector
      return self

    def setTimeout(self, timeout):
      self._timeout = timeout
      return self

    def setInitialSizeMembers(self, size):
      self._initial_size_members = size
      return self

    def setInitialSizePct(self, size):
      self._initial_size_pct = size
      return self

    def setDispatcherFactory(self, factory):
      self._dispatcher_factory = factory
      return self

    def build(self):
      if self._uri.startswith('zk://'):
        server_set_provider = None
      elif self._uri.startswith('tcp://'):
        uri = self._uri[6:]
        servers = uri.split(',')
        server_objs = []
        for s in servers:
          parts = s.split(':')
          server = self.Server(self.Endpoint(parts[0], int(parts[1])))
          server_objs.append(server)

        server_set_provider = functools.partial(StaticServerSetProvider,
          servers=server_objs)
      else:
        raise NotImplementedError("Invalid URI")

      pool = SingletonPool(
        self._name,
        server_set_provider,
        self._dispatcher_factory,
        self._selector,
        self._initial_size_members,
        self._initial_size_pct,
        self._timeout,
        self._dispatcher_factory.AreDispatchersSharable()
      )
      return Scales._WrapClient(self._client, pool)

  @staticmethod
  def newBuilder(Client):
    return Scales._ServiceBuilder(Client)
