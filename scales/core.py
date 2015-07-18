"""Scales core module."""

import collections
import functools
import inspect

from .constants import (SinkProperties, SinkRole)
from .dispatch import MessageDispatcher
from .loadbalancer.serverset import (
  StaticServerSetProvider,
  ZooKeeperServerSetProvider,
)
from .sink import TimeoutSinkProvider

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
    """Creates a proxy class that takes all method on Iface and proxies them to
    a dispatcher.

    Args:
      Iface - A class object implementing one or more thrift interfaces.
    """
    proxy_cls = ClientProxyBuilder._PROXY_TYPE_CACHE.get(Iface, None)
    if not proxy_cls:
      proxy_cls = ClientProxyBuilder._BuildServiceProxy(Iface)
      ClientProxyBuilder._PROXY_TYPE_CACHE[Iface] = proxy_cls
    return proxy_cls


class ScalesUriParser(object):
  """Default Uri Parser for scales.  Handles both tcp:// and zk:// URIs"""
  from .loadbalancer.zookeeper import Endpoint
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
      self._service = Iface
      self._name = Iface.__module__
      self._uri_parser = ScalesUriParser()
      self._uri = None
      self._timeout = 10
      self._server_set_provider = None
      self._client_provider = ClientProxyBuilder()
      self._properties = {}
      self._stack = []

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

    def SetTimeout(self, timeout):
      """Sets the default call timeout.

      Args:
        timeout - A timeout in seconds.
      """
      self._timeout = timeout
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
      """Sets the server set provider.  Must be called after SetUri(), or else
      SetUri() will overwrite the value set here.

      Args:
        server_set_provider - A subclass of ServerSetProvider.
      """

      self._server_set_provider = server_set_provider
      return self

    def SetName(self, name):
      """Sets the name of the service.  The name is used in logging and varz.

      Args:
        name - The friendly service name.
      """
      self._name = name
      return self

    def WithSink(self, sink_params):
      self._stack.append(sink_params)
      return self

    def InsertSink(self, idx, sink_params):
      self._stack.insert(idx, sink_params)
      return self

    def _Replace(self, sink_params, predicate):
      replace_idx = next((i for i, n in enumerate(self._stack)
                          if predicate(n)), -1)
      if replace_idx == -1:
        raise Exception("Unable to find sink to replace.")
      else:
        self._stack[replace_idx] = sink_params
      return self

    def ReplaceSink(self, cls_to_replace, sink_params):
      return self._Replace(sink_params, lambda n: isinstance(n, cls_to_replace))

    def ReplaceRole(self, role, sink_params):
      return self._Replace(sink_params, lambda n: n.Role == role)

    def Build(self):
      """Build a client given the current builder configuration.

      Returns:
        A proxy object with all methods of Iface.
      """
      for n, p in enumerate(self._stack[:-1]):
        p.next_provider = self._stack[n+1]

      # Add the server set provider to the properties of the load balancer in the
      # stack (if one exists)
      for n, p in enumerate(self._stack):
        if p.Role == SinkRole.LoadBalancer and p.sink_properties is not None:
          new_params_dct = p.sink_properties.__dict__.copy()
          new_params_dct['server_set_provider'] = self._server_set_provider
          p.sink_properties = p.PARAMS_CLASS(**new_params_dct)
          break

      timeout_sink = TimeoutSinkProvider()
      timeout_sink.next_provider = self._stack[0]

      properties = {
        SinkProperties.Label: self.name,
        SinkProperties.ServiceInterface: self._service,
      }
      properties.update(self._properties)

      Scales.SERVICE_REGISTRY[self.name] = (timeout_sink, properties)
      dispatcher = MessageDispatcher(
          self._service,
          timeout_sink,
          self._timeout,
          properties)

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
