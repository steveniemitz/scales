import collections
import functools

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

class TMuxTransport(TFramedTransport):
  def __init__(self, dispatcher_pool):
    self._dispatcher_pool = dispatcher_pool
    self._read_future = None
    TFramedTransport.__init__(self, None)

  def flush(self):
    payload = getattr(self, '_TFramedTransport__wbuf')
    setattr(self, '_TFramedTransport__wbuf', StringIO())
    _, dispatcher = self._dispatcher_pool.Get()
    self._read_future = dispatcher.SendDispatchMessage(payload)

  def readFrame(self):
    if self._read_future is None:
      raise Exception("Unexpected read!")
    else:
      msg = self._read_future.get()
      setattr(self, '_TFramedTransport__rbuf', msg.buf)
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

_MUXERS = {}

class MuxDispatcherProvider(object):
  def GetConnection(self, server, pool_name, health_cb, timeout):
    key = (server, pool_name)
    if key in _MUXERS:
      disp, cbs = _MUXERS[key]
    else:
      sock = THealthySocket(server.host, server.port, None, None, pool_name)
      disp = MuxMessageDispatcher(sock, timeout)
      cbs = set()
      _MUXERS[key] = (disp, cbs)

    if health_cb not in cbs:
      cbs.add(health_cb)
      disp.shutdown_result.rawlink(lambda ar: health_cb(server))

    return disp

  def IsConnectionFault(self, e):
    return isinstance(e,  TTransport.TTransportException)


class ThriftMux(object):
  @staticmethod
  def _WrapClient(Client, pool):
    class WrapperClient(Client):
      def __init__(self):
        pass

    def Wrap(orig_method):
      @functools.wraps(orig_method)
      def _Wrap(*args, **kwargs):
        mux = TMuxTransport(pool)
        prot = TBinaryProtocol.TBinaryProtocolAccelerated(mux)
        cli = Client(prot, prot)
        return orig_method(cli, *args, **kwargs)
      return _Wrap

    iface = next(b for b in Client.__bases__ if b.__name__ == 'Iface')
    wc = WrapperClient()
    methods = [m for m in dir(iface) if not m.startswith('__')]
    for m in methods:
      orig = getattr(Client, m)
      setattr(wc, m, Wrap(orig))
    return wc

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

    def build(self):
      if self._uri.startswith('zk://'):
        server_set_provider = None
      else:
        servers = self._uri.split(',')
        server_objs = []
        for s in servers:
          parts = s.split(':')
          server = self.Server(self.Endpoint(parts[0], int(parts[1])))
          server_objs.append(server)

        server_set_provider = functools.partial(StaticServerSetProvider,
          servers=server_objs)

      pool = SingletonPool(
        self._name,
        server_set_provider,
        MuxDispatcherProvider(),
        self._selector,
        self._initial_size_members,
        self._initial_size_pct,
        self._timeout,
        shareable_resources=True
      )
      return ThriftMux._WrapClient(self._client, pool)

  @staticmethod
  def newService(Client, uri):
    return ThriftMux.newBuilder(Client).setUri(uri).build()

  @staticmethod
  def newBuilder(Client):
    return ThriftMux._ServiceBuilder(Client)
