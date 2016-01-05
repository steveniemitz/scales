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
  def Close(self):
    """Close the provider and any resources associated.
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

  def Close(self):
    pass

  def GetServers(self):
    return self._servers


class ZooKeeperServerSetProvider(ServerSetProvider):
  """A ServerSetProvider that tracks servers in zookeeper."""
  from ..loadbalancer.zookeeper import ServerSet
  from kazoo.client import KazooClient
  from kazoo.handlers.gevent import SequentialGeventHandler

  def __init__(self,
      zk_servers_or_client,
      zk_path,
      zk_timeout=30.0,
      member_prefix='member_',
      member_factory=None,
      endpoint_name=None):
    """
    Args:
        zk_servers_or_client - Either a comma separated list of host:port ZK
                               servers, or an instance of a KazooClient.
        zk_path - The ZK path to discover services from.
        zk_timeout - The timeout to set on the ZK client.  Ignored if
                     zk_servers_or_client is a KazooClient.
        member_prefix - The prefix to match for children in the watched znode.
        member_factory - A callable to produce a Member object from a znode.
        endpoint_name - The name of the endpoint on the Member.  If None, the
                        endpoint is the serviceEndpoint in the znode data,
                        otherwise it is taken from the additionalEndpoints data
                        in the znode.
    """
    self._zk_client = None
    if isinstance(zk_servers_or_client, basestring):
      self._zk_client = self._GetZooKeeperClient(zk_servers_or_client, zk_timeout)
      self._owns_zk_client = True
    else:
      self._zk_client = zk_servers_or_client
      self._owns_zk_client = False
    self._zk_path = zk_path
    self._server_set = None
    self._member_prefix = member_prefix
    self._member_factory = member_factory
    self._endpoint_name = endpoint_name

  def _GetZooKeeperClient(self, zk_servers, zk_timeout):
    if self._zk_client:
      return self._zk_client
    else:
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

  def Close(self):
    if self._server_set:
      self._server_set.stop()
    if self._owns_zk_client and self._zk_client:
      self._zk_client.stop()

  def GetServers(self):
    if not self._server_set:
      raise Exception('Initialize() must be called first.')
    return self._server_set.get_members()

  @property
  def endpoint_name(self):
    return self._endpoint_name
