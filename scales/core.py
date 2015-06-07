"""Scales core module."""

import collections
import functools
import inspect

from .constants import SinkProperties
from .dispatch import MessageDispatcher
from .pool import (
  StaticServerSetProvider,
  ZooKeeperServerSetProvider,
)
from .sink import TimeoutSinkProvider
from .timer_queue import TimerQueue

# A timer queue for all async timeouts in scales.
GLOBAL_TIMER_QUEUE = TimerQueue()


class _ProxyBase(object):
  def __init__(self, dispatcher):
    self._dispatcher = dispatcher
    super(_ProxyBase, self).__init__()

  def __del__(self):
    self.DispatcherClose()

  def DispatcherOpen(self):
    self._dispatcher.Open()

  def DispatcherClose(self):
    self._dispatcher.Close()


class ClientProxyBuilder(object):
  """A class to generate proxy objects that can delegate calls to a
  message dispatcher.
  """
  _PROXY_TYPE_CACHE = {}

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

    is_user_method = lambda m: inspect.ismethod(m) and not inspect.isbuiltin(m)

    # Get all methods defined on the interface.
    iface_methods = { m[0]: ProxyMethod(*m)
                      for m in inspect.getmembers(Iface, is_user_method) }

    iface_methods.update({ m[0] + "_async": ProxyMethod(*m, async=True)
                           for m in inspect.getmembers(Iface, is_user_method) })

    # Create a proxy class to intercept the interface's methods.
    proxy = type(
      '_ScalesTransparentProxy<%s>' % Iface.__module__,
      (_ProxyBase, Iface),
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
    servers = uri.rstrip('/').split(',')
    server_objs = []
    for s in servers:
      host, port = s.split(':')
      server = self.Server(self.Endpoint(host, int(port)))
      server_objs.append(server)
    return StaticServerSetProvider(server_objs)

  def _HandleZooKeeper(self, uri):
    hosts, path = uri.split('/', 1)
    return ZooKeeperServerSetProvider(hosts, path)


class Scales(object):
  """Factory for scales clients."""

  SERVICE_REGISTRY = {}

  class ClientBuilder(object):
    """Builder for creating Scales clients."""

    def __init__(self, Iface):
      self._built = False
      self._service = Iface
      self._name = Iface.__module__
      self._uri_parser = ScalesUriParser()
      self._uri = None
      self._timeout = 10
      self._server_set_provider = None
      self._sink_provider = None
      self._load_balancer = None
      self._client_provider = ClientProxyBuilder()

    @property
    def name(self):
      return self._name

    @property
    def server_set_provider(self):
      return self._server_set_provider

    def SetUri(self, uri):
      """Sets the URI for this client.

      By default, uri may be in the form of:
        tcp://<host:port>[,...]
      or
        zk://<zk_host:zk:port>[,...]/full/zk/path
      """
      self._uri = uri
      self._server_set_provider = self._uri_parser.Parse(uri)
      return self

    def SetUriParser(self, parser):
      """Sets the URI parser for this builder.

      Uri parsers build a ServerSetProvider from a uri."""
      self._uri_parser = parser
      return self

    def SetPoolMemberSelector(self, selector):
      """Sets the pool member selector.

      Args:
        selector - An instance of a PoolMemberSelector or derived class.
      """
      self._selector = selector
      return self

    def SetTimeout(self, timeout):
      """Sets the default call timeout.

      Args:
        timeout - A timeout in seconds.
      """
      self._timeout = timeout
      return self

    def SetInitialSizeMembers(self, size):
      """Sets the initial size of the pool, in members.

      Args:
        size - The size of the pool.
      """
      self._initial_size_members = size
      return self

    def SetInitialSizePct(self, size):
      """Sets the initial size of the pool, as a ratio of the total number of
      members at initialization time.

      Args:
        size - A float between (0,1].
      """
      self._initial_size_pct = size
      return self

    def SetSinkProvider(self, message_sink_builder):
      """Sets the message sink builder.  Message sink builders are used to
      process messages.  See sink.py for a full description of message sinks.

      Args:
        message_sink_builder - An instance of a MessageSinkStackBuilder or derived class.
      """
      self._sink_provider = message_sink_builder
      return self

    def SetClientProvider(self, client_provider):
      """Sets the client provider.  Client providers are used to create the
      client proxy class returned from build().

      Args:
        client_provider - An instance of a ClientProxyBuilder.
      """
      self._client_provider = client_provider
      return self

    def SetServerSetProvider(self, server_set_provider):
      self._server_set_provider = server_set_provider
      return self

    def Build(self):
      """Build a client given the current builder configuration.

      Returns:
        A proxy object with all methods of Iface.
      """
      sink_provider = self._sink_provider.CreateProvider()

      timeout_sink = TimeoutSinkProvider()
      timeout_sink.next_provider = sink_provider

      properties = {
        SinkProperties.Service: self.name,
        SinkProperties.ServerSetProvider: self.server_set_provider,
        SinkProperties.Timeout: self._timeout
      }
      Scales.SERVICE_REGISTRY[self._service] = (timeout_sink, properties)
      dispatcher = MessageDispatcher(
          self._service,
          timeout_sink,
          properties)

      self._built = True
      proxy_cls = self._client_provider.CreateServiceClient(self._service)
      proxy_obj = proxy_cls(dispatcher)
      proxy_obj.DispatcherOpen()
      return proxy_obj

  @staticmethod
  def NewBuilder(Iface):
    """Creates a new client builder for a given interface.
    All methods on the interface will be proxied into the Scales dispatcher.

    Args:
      Iface - The interface to proxy.
    Returns:
      A client builder for the interface.
    """
    return Scales.ClientBuilder(Iface)
