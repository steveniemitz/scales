import collections
import functools
import inspect
import types

from cStringIO import StringIO
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TFramedTransport

from scales.dispatch import MessageDispatcher

from scales.pool import (
  SingletonPool,
  StaticServerSetProvider)

from scales.sink import PoolMemberSelectorTransportSink

class TScalesTransport(TFramedTransport):
  def __init__(self, dispatcher):
    self._dispatcher = dispatcher
    self._read_future = None
    TFramedTransport.__init__(self, None)

  def flush(self):
    payload = getattr(self, '_TFramedTransport__wbuf')
    setattr(self, '_TFramedTransport__wbuf', StringIO())
    self._read_future = self._dispatcher.SendDispatchMessage(payload)

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
  def _WrapClient(Client, dispatcher):
    def Proxy(orig_method):
      @functools.wraps(orig_method)
      def _Wrap(self, *args, **kwargs):
        mux = TScalesTransport(dispatcher)
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
    _POOLS = {}

    def _CreatePoolKey(self):
      return (
        self._name,
        self._server_set_provider.__class__,
        self._transport_sink_provider.__class__,
        self._selector.__class__,
        self._message_sink_provider.__class__,
        self._initial_size_members,
        self._initial_size_pct,
        self._timeout)

    def CreateMessageSink(self):
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
          self._transport_sink_provider.AreTransportsSharable()
        )
        self._POOLS[key] = pool

      message_stack = self._message_sink_provider.CreateMessageSink()
      transport_stack = [
        PoolMemberSelectorTransportSink(pool)
      ]
      sink_stack = message_stack + transport_stack
      for s in range(0, len(sink_stack) - 1):
        sink_stack[s].next_sink = sink_stack[s + 1]

      return sink_stack[0]

    def __init__(self, Client):
      self._built = False
      self._client = Client
      self._name = Client.__module__
      self._uri = None
      self._zk_servers = None
      self._selector = None
      self._timeout = 10
      self._initial_size_members = 0
      self._initial_size_pct = 0
      self._server_set_provider = None
      self._transport_sink_provider = None
      self._message_sink_provider = None

    def setUri(self, uri):
      self._uri = uri
      if self._uri.startswith('zk://'):
        self._server_set_provider = None
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

    def build(self):
      dispatcher = MessageDispatcher(self, self._timeout)
      self._built = True
      return Scales._WrapClient(self._client, dispatcher)

  @staticmethod
  def newBuilder(Client):
    return Scales._ServiceBuilder(Client)
