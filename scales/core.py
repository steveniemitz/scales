"""Scales core module."""

import collections
import functools
import inspect

from .dispatch import MessageDispatcher
from .pool import (
  RoundRobinPoolMemberSelector,
  SingletonPool,
  StaticServerSetProvider,
  ZooKeeperServerSetProvider
)
from .sink import (
  ClientFormatterSink,
  PooledTransportSink
)


class ClientProxyBuilder(object):
  """A class to generate proxy objects that can delegate calls to a
  message dispatcher.
  """
  _PROXY_TYPE_CACHE = {}

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
      ClientProxyBuilder._GetProxyName(Iface),
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
    proxy_cls = ClientProxyBuilder._PROXY_TYPE_CACHE.get(Iface, None)
    if not proxy_cls:
      proxy_cls = ClientProxyBuilder._BuildServiceProxy(Iface)
      ClientProxyBuilder._PROXY_TYPE_CACHE[Iface] = proxy_cls
    return proxy_cls


class ScalesUriParser(object):
  Endpoint = collections.namedtuple('Endpoint', 'host port')
  Server = collections.namedtuple('Server', 'service_endpoint')

  def __init__(self):
    self.handlers = {
      'tcp': self._HandleTcp,
      'zk': self._HandleZooKeeper
    }

  def Parse(self, uri):
    prefix, rest = uri.split('://', 1)
    if not prefix:
      raise Exception("Invalid URI")

    handler = self.handlers.get(prefix.lower(), None)
    if not handler:
      raise Exception("No handler found for prefix %s" % prefix)
    return handler(rest)

  def _HandleTcp(self, uri):
    servers = uri.split(',')
    server_objs = []
    for s in servers:
      parts = s.split(':')
      server = self.Server(self.Endpoint(parts[0], int(parts[1])))
      server_objs.append(server)
    return StaticServerSetProvider(server_objs)

  def _HandleZooKeeper(self, uri):
    hosts, path = uri.split('/', 1)
    return ZooKeeperServerSetProvider(hosts, path)


class Scales(object):
  """Factory for scales clients."""

  class ClientBuilder(object):
    """Builder for creating Scales clients."""
    _POOLS = {}

    def __init__(self, Iface):
      self._built = False
      self._service = Iface
      self._name = Iface.__module__
      self._uri_parser = ScalesUriParser()
      self._uri = None
      self._selector = RoundRobinPoolMemberSelector()
      self._timeout = 10
      self._initial_size_members = 0
      self._initial_size_pct = 0
      self._server_set_provider = None
      self._transport_sink_builder = None
      self._message_sink_builder = None
      self._pool = None
      self._client_provider = ClientProxyBuilder()

    class ScalesSinkStackBuilder(object):
      """Creates a full scales message sink stack given an arbitrary
      message sink stack.  This involves adding a pooling transport sink to the
      tail of the stack.
      """
      def __init__(self, pool, name, message_sink_provider):
        self._pool = pool
        self._name = name
        self._message_sink_provider = message_sink_provider

      def CreateSinkStack(self):
        """Create a sink stack using the given MessageSinkBuilder

        Returns:
          The head sink in the final stack.
        """
        sink_stack = self._message_sink_provider.CreateSinkStack(self._name)
        head_sink = sink_stack
        while sink_stack.next_sink:
          sink_stack = sink_stack.next_sink

        if not isinstance(sink_stack, ClientFormatterSink):
          raise Exception('The last sink in the message sink chain '
                          'must be a ClientFormatterSink')

        sink_stack.next_sink = PooledTransportSink(self._pool)
        return head_sink

    def _CreatePoolKey(self):
      return (
        self._name,
        self._uri,
        self._server_set_provider.__class__,
        self._transport_sink_builder.__class__,
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
          self._transport_sink_builder,
          self._selector,
          self._initial_size_members,
          self._initial_size_pct,
          self._transport_sink_builder.AreTransportsSharable())
        self._POOLS[key] = pool
      self._pool = pool

    def setUri(self, uri):
      """Sets the URI for this client.

      By default, uri may be in the form of:
        tcp://<host:port>[,...]
      or
        zk://<zk_host:zk:port>[,...]/full/zk/path
      """
      self._uri = uri
      self._server_set_provider = self._uri_parser.Parse(uri)
      return self

    def setUriParser(self, parser):
      """Sets the URI parser for this builder.

      Uri parsers build a ServerSetProvider from a uri."""
      self._uri_parser = parser
      return self

    def setPoolMemberSelector(self, selector):
      """Sets the pool member selector.

      Args:
        selector - An instance of a PoolMemberSelector or derived class.
      """
      self._selector = selector
      return self

    def setTimeout(self, timeout):
      """Sets the default call timeout.

      Args:
        timeout - A timeout in seconds.
      """
      self._timeout = timeout
      return self

    def setInitialSizeMembers(self, size):
      """Sets the initial size of the pool, in members.

      Args:
        size - The size of the pool.
      """
      self._initial_size_members = size
      return self

    def setInitialSizePct(self, size):
      """Sets the initial size of the pool, as a ratio of the total number of
      members at initialization time.

      Args:
        size - A float between (0,1].
      """
      self._initial_size_pct = size
      return self

    def setTransportSinkBuilder(self, transport_sink_builder):
      """Sets the transport sink builder.  Transport sink builders are used to
      process serialized messages.  See sink.py for a full description of
      transport sinks.

      Args:
        transport_sink_builder - An instance of a TransportSinkStackBuilder or derived class.
      """
      self._transport_sink_builder = transport_sink_builder
      return self

    def setMessageSinkBuilder(self, message_sink_builder):
      """Sets the message sink builder.  Message sink builders are used to
      process messages.  See sink.py for a full description of message sinks.

      Args:
        message_sink_builder - An instance of a MessageSinkStackBuilder or derived class.
      """
      self._message_sink_builder = message_sink_builder
      return self

    def setClientProvider(self, client_provider):
      """Sets the client provider.  Client providers are used to create the
      client proxy class returned from build().

      Args:
        client_provider - An instance of a ClientProxyBuilder.
      """
      self._client_provider = client_provider
      return self

    def setServerSetProvider(self, server_set_provider):
      self._server_set_provider = server_set_provider
      return self

    def build(self):
      """Build a client given the current builder configuration.

      Returns:
        A proxy object with all methods of Iface.
      """
      if not self._pool:
        self._BuildPool()

      dispatcher = MessageDispatcher(
          self._service,
          self.ScalesSinkStackBuilder(self._pool, self._name, self._message_sink_builder),
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
