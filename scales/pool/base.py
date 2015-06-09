import logging

from abc import (
  ABCMeta,
  abstractmethod
)

from ..message import MethodReturnMessage
from ..sink import ClientMessageSink

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


class PoolSink(ClientMessageSink):
  """A pool maintains a set of zero or more sinks to a single endpoint."""

  def __init__(self, sink_provider, properties):
    self._properties = properties
    self._sink_provider = sink_provider
    super(PoolSink, self).__init__()

  @abstractmethod
  def _Get(self):
    """To be overridden by subclasses.  Called to get a sink from the pool.

    Returns:
      A sink.
    """
    raise NotImplementedError()

  @abstractmethod
  def _Release(self, sink):
    """To be overridden by subclasses.  Called when a sink has completed
    processing.

    Args:
      sink - A sink that had been previously returned from _Get()
    """
    raise NotImplementedError()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Gets a ClientChannelSink from the pool (asynchronously), and continues
    processing the message on it.
    """
    try:
      sink = self._Get()
    except Exception as e:
      ex_msg = MethodReturnMessage(error=e)
      sink_stack.AsyncProcessResponseMessage(ex_msg)
      return

    sink_stack.Push(self, sink)
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    """Returns the sink (in 'context') to the pool and delegates to the next sink.
    """
    self._Release(context)
    sink_stack.AsyncProcessResponse(stream, msg)
