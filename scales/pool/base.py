import logging

from abc import (
  ABCMeta,
  abstractmethod
)

import gevent

from ..message import MethodReturnMessage
from ..sink import (
  ClientChannelSink
)

ROOT_LOG = logging.getLogger("scales")

class ServerSetProvider(object):
  """Base class for providing a set of servers, as well as optionally
  notifying the pool of servers joining and leaving the set."""
  __metaclass__ = ABCMeta

  @abstractmethod
  def Initialize(self, on_join, on_leave):
    """Initialize the provider.
    This method is called before any calls to GetServers().

     Args:
      on_join - A function to be called when a server joins the set.
      on_leave - A function to be called when a server leaves the set.
    """
    raise NotImplementedError()

  @abstractmethod
  def GetServers(self):
    """Get all the current servers in the set.

    Returns:
      An iterable of servers.
    """
    raise NotImplementedError()


class StaticServerSetProvider(ServerSetProvider):
  """A ServerSetProvider that returns a static set of servers."""

  def __init__(self, servers):
    """Initializes the set with a static list of servers.

    Args:
      servers - An iterable of servers.
    """
    self._servers = servers

  def Initialize(self, on_join, on_leave):
    pass

  def GetServers(self):
    return self._servers


class ZooKeeperServerSetProvider(ServerSetProvider):
  """A ServerSetProvider that tracks servers in zookeeper."""
  from ..loadbalancer.zookeeper import ServerSet
  from kazoo.client import KazooClient
  from kazoo.handlers.gevent import SequentialGeventHandler

  def __init__(self, zk_servers, zk_path, zk_timeout=30):
    self._zk_client = self._GetZooKeeperClient(zk_servers, zk_timeout)
    self._zk_path = zk_path
    self._server_set = None

  def _GetZooKeeperClient(self, zk_servers, zk_timeout):
    return self.KazooClient(
      hosts=zk_servers,
      timeout=zk_timeout,
      handler=self.SequentialGeventHandler(),
      randomize_hosts=True)

  def Initialize(self, on_join, on_leave):
    self._zk_client.start()
    self._server_set = self.ServerSet(
        self._zk_client, self._zk_path, on_join, on_leave)

  def GetServers(self):
    if not self._server_set:
      raise Exception('Initialize() must be called first.')

    return self._server_set.get_members()


class PoolChannelSink(ClientChannelSink):
  __metaclass__ = ABCMeta

  """A pool maintains a set of zero or more transport sinks
  to a single endpoint."""
  def __init__(self, name, endpoint, sink_provider):
    self._name = name
    self._endpoint = endpoint
    self._sink_provider = sink_provider
    super(PoolChannelSink, self).__init__()

  def __call__(self, *args, **kwargs):
    next_sink = self._Get()
    return next_sink(*args, **kwargs).Always(self._Release)

  @abstractmethod
  def _Get(self):
    raise NotImplementedError()

  @abstractmethod
  def _Release(self, sink):
    raise NotImplementedError()

  @abstractmethod
  def Open(self):
    raise NotImplementedError()

  @abstractmethod
  def Close(self):
    raise NotImplementedError()

  def _AsyncProcessRequestCallback(self, sink_stack, msg, stream, headers):
    """Continues processing of AsyncProcessRequest"""
    try:
      sink = self._Get()
    except Exception as e:
      excr = MethodReturnMessage(error=e)
      sink_stack.AsyncProcessResponse(None, excr)
      return

    sink_stack.Push(self, sink)
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Gets a ClientChannelSink from the pool (asynchronously), and continues
    the sink chain on it.
    """

    # Getting a member of the pool may involve a blocking operation.  In order
    # to maintain a fully asynchronous stack, the rest of the chain is executed
    # on a worker greenlet.
    gevent.spawn(self._AsyncProcessRequestCallback, sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    """Returns the ClientChannelSink to the pool and delegates to the next sink.

    Args:
      sink_stack - The sink stack.
      context - A tuple supplied from AsyncProcessRequest of (shard, channel sink).
      stream - The stream being processed.
    """
    self._Release(context)
    sink_stack.AsyncProcessResponse(stream, msg)
