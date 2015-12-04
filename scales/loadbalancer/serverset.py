from abc import (ABCMeta, abstractmethod)

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

  @property
  def endpoint_name(self):
    return None


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

  def __init__(self,
      zk_servers,
      zk_path,
      zk_timeout=30,
      member_prefix='member_',
      member_factory=None,
      endpoint_name=None):
    self._zk_client = self._GetZooKeeperClient(zk_servers, zk_timeout)
    self._zk_path = zk_path
    self._server_set = None
    self._member_prefix = member_prefix
    self._member_factory = member_factory
    self._endpoint_name = endpoint_name

  def _GetZooKeeperClient(self, zk_servers, zk_timeout):
    return self.KazooClient(
      hosts=zk_servers,
      timeout=zk_timeout,
      handler=self.SequentialGeventHandler(),
      randomize_hosts=True)

  def _MemberFilter(self, node):
    return node.startswith(self._member_prefix)

  def Initialize(self, on_join, on_leave):
    self._zk_client.start()
    self._server_set = self.ServerSet(
      self._zk_client, self._zk_path, on_join, on_leave,
      self._MemberFilter, self._member_factory)

  def GetServers(self):
    if not self._server_set:
      raise Exception('Initialize() must be called first.')

    return self._server_set.get_members()

  @property
  def endpoint_name(self):
    return self._endpoint_name
