"""A pool that tracks a set of resources as well as their health."""

import collections
import itertools
import logging
import random
import traceback

from abc import (
  ABCMeta,
  abstractmethod
)
from contextlib import contextmanager

import gevent
from gevent.event import Event

from ..varz import (
  Counter,
  Gauge,
  VarzBase
)
from ..sink import ClientChannelTransportSink

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
  from .zookeeper import ServerSet
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


class AcquirePoolMemeberException(Exception):
  """An exception raised when all members of the pool have failed."""
  pass


class PoolMemberSelector(object):
  """Base class for selecting a pool member from a set of healthy servers.
  PoolMemberSelectors are notified when healthy servers join or leave the pool.
  """
  __metaclass__ = ABCMeta

  @abstractmethod
  def GetNextMember(self):
    """Get the next member to use from the pool.

    Returns:
      A server from the healthy set.
    """
    raise NotImplementedError()

  @abstractmethod
  def OnHealthyMembersChanged(self, all_healthy_servers):
    """Called by the pool when the set of healthy servers changes.

    Args:
      all_healthy_servers - An iterable of all currently healthy servers.
    """
    raise NotImplementedError()


class RoundRobinPoolMemberSelector(PoolMemberSelector):
  """PoolMemberSelector that cycles through all healthy members in order."""

  def __init__(self):
    self._next_server = None

  def OnHealthyMembersChanged(self, all_healthy_servers):
    healthy_servers = list(all_healthy_servers)
    random.shuffle(healthy_servers)
    self._next_server = itertools.cycle(healthy_servers)

  def GetNextMember(self):
    try:
      return self._next_server.next()
    except StopIteration:
      return None


class SingletonPool(object):
  """A pool of resources."""

  POOL_LOGGER = ROOT_LOG.getChild('SingletonPool')
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.SingletonPool'
    _VARZ = {
      'num_servers': Gauge,
      'num_healthy_servers': Gauge,
      'num_unhealthy_servers': Gauge,
      'resources_created': Counter,
      'all_members_failed': Counter
    }

  def __init__(
      self,
      pool_name,
      server_set_provider,
      connection_provider,
      member_selector,
      initial_size_min_members=0,
      initial_size_factor=0,
      shareable_resources=False):
    """Initializes the pool.

    Args:
      pool_name - The name of this pool to be used in logging.
      server_set_provider - A class that provides a GetServers method that
        takes two parameters, on_join and on_leave, and returns a server set.
        A server set must be iterable.
      connection_provider - A class that provides two methods:
        GetConnection(): Must return a "connection" that implements:
          - open()
          - isOpen()
          - close()
          - testConnection()

        IsConnectionFault: Must take an exception an return true if that
          exception was caused by the connection failing.
      member_selector - An instance of a PoolMemberSelector.
      initial_size_min_members - The absolute minimum number of connections in
        the pool at startup.  The pool maybe grow or shrink after startup.
      initial_size_factor - The minimum number of connections in the pool at
        startup as a multiple of the total number of healthy servers.  For
        example, if there are 50 members in the ZK pool, and
        initial_size_factor = .5, 25 connections will be created at startup.
        The pool size is the maximum of initial_size_min_members and
        initial_size_factor.
      shareable_resources - If True, connections are never removed from the pool,
        and instead are returned to the tail.  This should be used if the
        underlying resources are safe to share.
    """
    self._init_done = Event()
    self._shareable_resources = shareable_resources
    self._connection_provider = connection_provider
    self._member_selector = member_selector
    self._pool_name = pool_name
    self._varz = self.Varz(pool_name)
    self.LOG = self.POOL_LOGGER.getChild('[%s]' % pool_name)
    self.LOG.info("Creating SingletonPool")
    server_set_provider.Initialize(
        self._OnServerSetJoin,
        self._OnServerSetLeave)
    self._server_set = server_set_provider.GetServers()
    self._servers = set(m.service_endpoint for m in self._server_set)
    self._healthy_servers = self._servers.copy()
    self._socket_queue = collections.deque()
    self._OnHealthyServersChanged()
    self._init_done.set()
    self._ScheduleOperationWithPeriod(10, self._Reset)
    self._PrepopulatePool(initial_size_min_members, initial_size_factor)

  @staticmethod
  def _ScheduleOperationWithPeriodWorker(period, operation):
    while True:
      gevent.sleep(period)
      operation()

  @staticmethod
  def _ScheduleOperationWithPeriod(period, operation):
    """Schedule an operation to happen ever [period] seconds

    Args:
      period - The period to run the operation on.
      operation - A callable to run.
    """
    gevent.spawn(SingletonPool._ScheduleOperationWithPeriodWorker, period, operation)

  def _PrepopulatePool(self, initial_size_min_members, initial_size_factor):
    """Pre-populate the connection pool at startup.  See ctor for parameters.
    """
    calculated_initial_min_size = max(
      initial_size_min_members,
      len(self._healthy_servers) * initial_size_factor)
    for i in range(calculated_initial_min_size):
      shard = self._GetNextServer()
      socket = self._CreateConnection(shard)
      self._UncheckedReturn(shard, socket)

  def _AddServer(self, instance):
    """Adds a servers to the set of servers available to the connection pool.
    Note: no new connections are created at this time.

    Args:
      instance - A Member object to be added to the pool.
    """
    if not instance.service_endpoint in self._servers:
      self._servers.add(instance.service_endpoint)
      self._healthy_servers.add(instance.service_endpoint)
      self._OnHealthyServersChanged()

  def _RemoveServer(self, instance):
    """Removes a server from the connection pool.  Outstanding connections will
    be closed lazily.

    Args:
      instance - A Member object to be removed from the pool.
    """
    self._servers.discard(instance.service_endpoint)
    self._healthy_servers.discard(instance.service_endpoint)
    self._OnHealthyServersChanged()

  def _GetNextServer(self):
    """Returns the next available healthy server based on the pool
     member selector.

    Returns:
      The Member selected.
    """
    member = self._member_selector.GetNextMember()
    if not member:
      raise AcquirePoolMemeberException("No members were available for pool")
    return member

  def _UpdatePoolSizeVarz(self):
    """Updates varz for num_servers, num_healthy_servers,
    and num_unhealthy_servers.
    """
    num_healthy_servers = len(self._healthy_servers)
    num_servers = len(self._servers)
    self._varz.num_healthy_servers(num_healthy_servers)
    self._varz.num_servers(num_servers)
    self._varz.num_unhealthy_servers(num_servers - num_healthy_servers)

  def _OnHealthyServersChanged(self):
    """Callback raised when a server becomes healthy or unhealthy."""
    self._UpdatePoolSizeVarz()
    self._member_selector.OnHealthyMembersChanged(self._healthy_servers)

  def _HealthCallback(self, shard):
    """Called by connections when they decide their connection is unhealthy.
    Calling this marks the server unhealthy in the pool.
    Existing connections to the server are lazily closed / removed.
    """
    self._healthy_servers.discard(shard)
    self._OnHealthyServersChanged()

  def _Reset(self):
    """Checks all unhealthy servers and if healthy, adds connections to them
    back to the connection pool.

    This function is called periodically on a background greenlet.
    """
    unhealthy_servers = self._servers - self._healthy_servers
    for server in unhealthy_servers:
      try:
        socket = self._CreateConnection(server)
        socket.open()
        self._UncheckedReturn(server, socket)
        self._healthy_servers.add(server)
        self._OnHealthyServersChanged()
      except Exception as ex:
        if not self._connection_provider.IsConnectionFault(ex):
          self.LOG.exception("Error checking health for server %s" % server)

  def _CreateConnection(self, shard):
    """Calls the connection provider to create a new connection to a given
    server.  Returned connections are not opened.

    Returns:
      A new, unopened connection.
    """
    self._varz.resources_created()
    sink = self._connection_provider.CreateSinkStack(shard, self._pool_name)
    next_sink = sink
    while next_sink.next_sink:
      next_sink = next_sink.next_sink

    if not isinstance(next_sink, ClientChannelTransportSink):
      raise Exception(
        "CreateSinkStack must return an instance of a ClientChannelTransportSink")

    # TODO: Note: if the transport sink provider returns the same sink multiple times
    # (for example, the thriftmux provider keeps its own pool), the _HealthCallback
    # may be fired multiple times.  We could prevent this, but _HealthCallback
    # should be idempotent.
    next_sink.shutdown_result.rawlink(lambda ar: self._HealthCallback(shard))
    return sink

  def Get(self):
    """Gets the next server from the pool.

    Returns:
      A (member, resource) tuple.  This same tuple must be passed to Return()
      when it's no longer being used.

    Throws:
      AcquirePoolMemberException if the pool has no healthy servers.
      Exception if an unexpected error occurs unrelated to server connectivity.
    """
    num_retries = 0
    max_retries = max(len(self._healthy_servers) / 2, 1)
    last_exception = None
    while True:
      if any(self._socket_queue):
        # There's an available socket in the pool, use that.
        shard, socket = self._socket_queue.popleft()
        # It's possible the server this socket is using was marked unhealthy.
        # If so, close the socket and get a new one.
        if shard not in self._healthy_servers:
          try:
            socket.close()
          # it doesn't really matter if close fails, we were already trying
          # to get rid of it...
          except: pass
          continue
      else:
        shard = self._GetNextServer()
        socket = self._CreateConnection(shard)

      # Return the connection immediately if no_pop == True,
      if self._shareable_resources:
        self._UncheckedReturn(shard, socket)

      # We get to this point if either:
      #  - The socket was dequeued and still healthy or...
      #  - We created a new socket because there were none there.
      # In both cases try to open it now (if it's not already).
      try:
        if not socket.isOpen():
          socket.open()
          self.LOG.info("Opened socket for shard %s" % str(shard))

        # Check if the socket is healthy by running a non-blocking select on it.
        if not socket.testConnection():
          self.LOG.error("Checking connection failed for shard %s, marking unhealthy"
                    % str(shard))
          self._HealthCallback(shard)
        else:
          return shard, socket
      except AcquirePoolMemeberException:
        self.LOG.error("All nodes failed for pool.")
        self._varz.all_members_failed()
        raise
      except Exception as ex:
        last_exception = traceback.format_exc()
        if not self._connection_provider.IsConnectionFault(ex):
          raise
        else:
          # Mark the server unhealthy as we were unable to connect to it.
          self.LOG.error("Unable to initialize pool member %s for pool." %
              str(shard))
          self._HealthCallback(shard)

      # If we got here, we were unable to open a connection with
      # Only retry num_healthy_servers / 2 times before failing.
      num_retries+=1
      if num_retries > max_retries:
        raise AcquirePoolMemeberException(
          "Unable to acquire a node in %d tries for pool." % num_retries)

  def Return(self, server, socket):
    """Returns a resource to the pool.

    Args:
      server, socket - The tuple returned from Get()
    """
    if not self._shareable_resources:
      self._UncheckedReturn(server, socket)

  def _UncheckedReturn(self, server, socket):
    """Returns a resource to the pool.

    Note: This does not check if the pool is configured for sharable resources
    and always adds it back to the queue.

    Args:
      server, socket - The tuple returned from Get()
    """
    if server in self._servers:
      self._socket_queue.append((server, socket))

  @contextmanager
  def SafeGet(self):
    """A Context Manager for getting a resource from the pool and then
    returning it.
    """
    shard, socket = None, None
    try:
      shard, socket = self.Get()
      yield socket
    finally:
      if socket:
        self.Return(shard, socket)

  ## ------- ZooKeeper methods --------
  def _OnServerSetJoin(self, instance):
    """Invoked when an instance joins the cluster (in ZooKeeper).

    Args:
      instance - Instance added to the cluster.
    """
    # callbacks from the ServerSet are delivered serially, so we can guarentee
    # that once this unblocks, we'll still get the notifications delivered in
    # the order that they arrived.  Ex: OnJoin(a) -> OnLeave(a)
    self._init_done.wait()
    # OnJoin notifications are delivered at startup, however we already
    # pre-populate our copy of the ServerSet, so it's fine to ignore duplicates.
    if instance.service_endpoint in self._servers:
      return

    self._AddServer(instance)
    self.LOG.info("Instance joined (%d members)" % len(self._servers))

  def _OnServerSetLeave(self, instance):
    """Invoked when an instance leaves the cluster.

    If the instance leaving the cluster is the chosen shard,
    then the connections will be reset.

    Args:
      instance - Instance leaving the cluster.
    """
    self._init_done.wait()
    self._RemoveServer(instance)

    self.LOG.info("Instance left (%d members)" % len(self._servers))
    ## ------- /ZooKeeper methods --------
