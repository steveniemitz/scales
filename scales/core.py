"""Scales core module."""

import collections
import functools
import inspect

from .dispatch import MessageDispatcher
from .pool import (
  RoundRobinPoolMemberSelector,
  SingletonPool,
  StaticServerSetProvider,
  ZooKeeperServerSetProvider)
from .sink import PooledTransportSink


class ClientProxyProvider(object):
  _PROXY_CACHE = {}

  @staticmethod
  def _GetProxyName(Iface):
    return '_ScalesTransparentProxy<%s>' % Iface.__module__

  @staticmethod
  def _BuildServiceProxy(Iface):
    """Build a proxy class that intercepts all user methods on [Iface]
    and routes them to a message dispatcher.

    Args:
      Iface - An interface to proxy
    """

    def ProxyMethod(method_name, orig_method, async=False):
      @functools.wraps(orig_method)
      def _ProxyMethod(self, *args, **kwargs):
        ar = self._dispatcher.DispatchMethodCall(method_name, args, kwargs)
        return ar if async else ar.get()
      return _ProxyMethod

    def _ProxyInit(self, dispatcher):
      self._dispatcher = dispatcher

    is_user_method = lambda m: inspect.ismethod(m) and not inspect.isbuiltin(m)

    # Get all methods defined on the interface.
    iface_methods = { m[0]: ProxyMethod(*m)
                      for m in inspect.getmembers(Iface, is_user_method) }

    iface_methods.update({ m[0] + "_async": ProxyMethod(*m, async=True)
                           for m in inspect.getmembers(Iface, is_user_method) })
    iface_methods.update({ '__init__': _ProxyInit })

    # Create a proxy class to intercept the interface's methods.
    proxy = type(
      ClientProxyProvider._GetProxyName(Iface),
      (Iface, object),
      iface_methods)
    return proxy

  @staticmethod
  def CreateServiceClient(Iface):
    """Creates a proxy class that takes all method on Client
    and sends them to a dispatcher.

    Args:
      Iface - A class object implementing one or more thrift interfaces.
      dispatcher - An instance of a MessageDispatcher.
    """
    proxy_cls = ClientProxyProvider._PROXY_CACHE.get(Iface, None)
    if not proxy_cls:
      proxy_cls = ClientProxyProvider._BuildServiceProxy(Iface)
      ClientProxyProvider._PROXY_CACHE[Iface] = proxy_cls
    return proxy_cls


class Scales(object):
  """Factory for scales clients."""

  class ClientBuilder(object):
    Endpoint = collections.namedtuple('Endpoint', 'host port')
    Server = collections.namedtuple('Server', 'service_endpoint')
    _POOLS = {}

    def __init__(self, Iface):
      self._built = False
      self._service = Iface
      self._name = Iface.__module__
      self._uri = None
      self._zk_servers = None
      self._selector = RoundRobinPoolMemberSelector()
      self._timeout = 10
      self._initial_size_members = 0
      self._initial_size_pct = 0
      self._server_set_provider = None
      self._transport_sink_provider = None
      self._message_sink_provider = None
      self._pool = None
      self._client_provider = ClientProxyProvider()

    class ScalesSinkStackBuilder(object):
      def __init__(self, pool, name, message_sink_provider):
        self._pool = pool
        self._name = name
        self._message_sink_provider = message_sink_provider

      def CreateSinkStack(self):
        message_stack = self._message_sink_provider.CreateMessageSinks(self._name)
        transport_stack = [
          PooledTransportSink(self._pool)
        ]
        sink_stack = message_stack + transport_stack
        for s in range(0, len(sink_stack) - 1):
          sink_stack[s].next_sink = sink_stack[s + 1]
        return sink_stack[0]

    def _CreatePoolKey(self):
      return (
        self._name,
        self._uri,
        self._server_set_provider.__class__,
        self._transport_sink_provider.__class__,
        self._selector.__class__,
        self._initial_size_members,
        self._initial_size_pct)

    def _BuildPool(self):
      key = self._CreatePoolKey()
      pool = self._POOLS.get(key, None)
      if not pool:
        pool = SingletonPool(
          self._name,
          self._server_set_provider,
          self._transport_sink_provider,
          self._selector,
          self._initial_size_members,
          self._initial_size_pct,
          self._transport_sink_provider.AreTransportsSharable())
        self._POOLS[key] = pool
      self._pool = pool

    def setUri(self, uri):
      self._uri = uri
      if self._uri.startswith('zk://'):
        uri = uri[5:]
        hosts, path = uri.split('/', 1)
        self._server_set_provider = ZooKeeperServerSetProvider(hosts, path)
      elif self._uri.startswith('tcp://'):
        uri = self._uri[6:]
        servers = uri.split(',')
        server_objs = []
        for s in servers:
          parts = s.split(':')
          server = self.Server(self.Endpoint(parts[0], int(parts[1])))
          server_objs.append(server)

        self._server_set_provider = StaticServerSetProvider(server_objs)
      else:
        raise NotImplementedError("Invalid URI")
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

    def setTransportSinkProvider(self, transport_sink_provider):
      self._transport_sink_provider = transport_sink_provider
      return self

    def setMessageSinkProvider(self, message_sink_provider):
      self._message_sink_provider = message_sink_provider
      return self

    def setClientProvider(self, client_provider):
      self._client_provider = client_provider
      return self

    def build(self):
      if not self._pool:
        self._BuildPool()

      dispatcher = MessageDispatcher(
          self._service,
          self.ScalesSinkStackBuilder(self._pool, self._name, self._message_sink_provider),
          self._timeout)

      self._built = True
      proxy_cls = self._client_provider.CreateServiceClient(self._service)
      return proxy_cls(dispatcher)

  @staticmethod
  def newBuilder(Iface):
    """Creates a new client builder for a given interface.
    All methods on the interface will be proxied into the Scales dispatcher.

    Args:
      Iface - The interface to proxy.
    Returns:
      A client builder for the interface.
    """
    return Scales.ClientBuilder(Iface)
