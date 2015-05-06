"""A socket pool that is backed by service discovery running on Aurora.

Internally, Aurora uses Zookeeper for registering services.
"""

import collections
from contextlib import contextmanager
import functools
import itertools
import logging
import posixpath
import random

LOG = logging.getLogger(__name__)

_UNSET = '<unset>'

def ScheduleOperationWithPeriodWorker(period, operation):
  import gevent
  while True:
    gevent.sleep(period)
    operation()

def ScheduleOperationWithPeriod(period, operation):
  import gevent
  gevent.spawn(ScheduleOperationWithPeriodWorker, period, operation)

class AcquireResourceException(Exception): pass

class SingletonPool(object):
  """A connection pool backed by service discovery running on Aurora.
  """
  def __init__(
      self,
      pool_name,
      server_set_provider,
      connection_provider,
      initial_size_min_members=0,
      initial_size_factor=0,
      timeout_ms=None,
      no_pop=False):
    """Initializes the resource pool.

    connection_provider must implement GetConnection() and IsConnectionFault().
      GetConnection() must return a "connection" that implements:
        - open()
        - isOpen()
        - close()

      IsConnectionFault must take an excpetion an return true if that exception
        was caused by the connection failing.

    Args:
      aurora_zk_client - Client to Aurora's Zookeeper service.
      service_discovery_path - The root service discovery path in ZK.
      aurora_zk_path - The ZK path relative to the service_discovery_path.
      connection_provider - A class that provides two methods:
        GetConnection(): Must return a "connection" that implements:
          - open()
          - isOpen()
          - close()

        IsConnectionFault: Must take an excpetion an return true if that
          exception was caused by the connection failing.
      server_set_class - Class used to manage the set of servers. Used
                          primarily to mock out the ZK based ServerSet.
      initial_size_min_members - The absolute minimum number of connections in
        the pool at startup.  The pool maybe grow or shrink after startup.
      initial_size_factor - The minimum number of connections in the pool at
        startup as a multiple of the total number of healthy servers.  For
        example, if there are 50 members in the ZK pool, and
        initial_size_factor = .5, 25 connections will be created at startup.
        The pool size is the maximum of initial_size_min_members and
        initial_size_factor.
      timeout_ms - The timeout for connections in the pool, in milliseconds.
        If none, connections never time out.
    """
    self._no_pop = no_pop
    self._initialized = False
    self._connection_provider = connection_provider
    self._socket_timeout_ms = timeout_ms
    self._pool_name = pool_name
    LOG.info("Creating SingletonPool for %s" % pool_name)
    self.server_set = server_set_provider(
      on_join=self._OnServerSetJoin,
      on_leave=self._OnServerSetLeave)

    from gevent.event import Event
    self._init_done = Event()
    self._servers = set(m.service_endpoint for m in self.server_set)
    self._healthy_servers = self._servers.copy()
    self._socket_queue = collections.deque()
    self._OnHealthyServersChanged()
    self._init_done.set()
    ScheduleOperationWithPeriod(10, self._Reset)
    self._PrepopulatePool(initial_size_min_members, initial_size_factor)

  def _PrepopulatePool(self, initial_size_min_members, initial_size_factor):
    """Pre-populate the connection pool at startup.  See ctor for parameters.
    """
    calculated_initial_min_size = max(
      initial_size_min_members,
      len(self._healthy_servers) * initial_size_factor)
    for i in range(calculated_initial_min_size):
      shard = self._GetNextServer()
      socket = self._CreateConnection(shard)
      self.Return(shard, socket)

  def _AddServer(self, instance):
    """Adds a servers to the set of servers available to the connection pool.
    Note: no new connections are created at this time.
    """
    if not instance.service_endpoint in self._servers:
      self._servers.add(instance.service_endpoint)
      self._healthy_servers.add(instance.service_endpoint)
      self._OnHealthyServersChanged()

  def _RemoveServer(self, instance):
    """Removes a server from the connection pool.  Outstanding connections will
    be closed lazily.
    """
    self._servers.discard(instance.service_endpoint)
    self._healthy_servers.discard(instance.service_endpoint)
    self._OnHealthyServersChanged()

  def _GetNextServer(self):
    """To be implemented by sub-classes.  This method is called when there are
      no connections availble to serve a GetConnection() request.

    Returns:
      A new, non-open connection.
    """
    raise NotImplementedError()

  def _OnHealthyServersChanged(self):
    """To be optionally implemented by sub-classes.  This method is called when
    the set of healthy servers in the pool changes.  Either because new members
    joined, existing members left, or a member was marked unhealthy.
    """
    pass

  def _HealthCallback(self, shard):
    """Called by connections when they decide their connection is unhealhy.
    Calling this marks the server unhealhy in the pool.
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
        self.Return(server, socket)
        self._healthy_servers.add(server)
        self._OnHealthyServersChanged()
      except Exception as ex:
        if not self._connection_provider.IsConnectionFault(ex):
          LOG.exception("Error checking health for server %s" % server)

  def _CreateConnection(self, shard):
    """Calls the connection provider to create a new connection to a given
    server.  Returned connections are not opened.

    Returns:
      A new, unopened connection.
    """
    return self._connection_provider.GetConnection(
      shard,
      self._HealthCallback,
      self._socket_timeout_ms)

  def Get(self):
    num_retries = 0
    max_retries = max(len(self._healthy_servers) / 2, 1)
    while True:
      if any(self._socket_queue):
        # There's an available socket in the pool, use that.
        shard, socket = self._socket_queue.popleft()
        # It's possible the server this socket is using was marked unhealthy.
        # If so, close the socekt and get a new one.
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
      if self._no_pop:
        self.Return(shard, socket)

      # We get to this point if either:
      #  - The socket was dequeued and still healthy or...
      #  - We created a new socket because there were none there.
      # In both cases try to open it now (if it's not already).
      try:
        if not socket.isOpen():
          socket.open()
          LOG.info("Created socket for shard %s" % str(shard))

        # Check if the socket is healthy by running a non-blocking select on it.
        if not socket.testConnection():
          LOG.error("Checking connection failed for shard %s, marking unhealhy"
                    % str(shard))
          self._HealthCallback(shard)
        else:
          return shard, socket
      except AcquireResourceException:
        LOG.error("All nodes failed for pool %s." % self._pool_name)
        raise
      except Exception as ex:
        if not self._connection_provider.IsConnectionFault(ex):
          raise
        else:
          # Mark the server unhealthy as we were unable to connect to it.
          self._HealthCallback(shard)

      # If we got here, we were unable to open a connection with
      # Only retry num_healhy_servers / 2 times before failing.
      num_retries+=1
      if num_retries > max_retries:
        raise AcquireResourceException(
          "Unable to acquire a node in %d tries for pool %s." % (
              num_retries, self._pool_name))

  def Return(self, server, socket):
    if server in self._servers:
      self._socket_queue.append((server, socket))

  @contextmanager
  def SafeGetConnection(self):
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
    LOG.info("Instance joined [%s] (%d members)" % (
      self._pool_name, len(self._servers)))

  def _OnServerSetLeave(self, instance):
    """Invoked when an instance leaves the cluster.

    If the instance leaving the cluster is the chosen shard,
    then the connections will be reset.

    Args:
      instance - Instance leaving the cluster.
    """
    self._init_done.wait()
    self._RemoveServer(instance)

    LOG.info("Instance left [%s] (%d members)" % (
      self._pool_name, len(self._servers)))
    ## ------- /ZooKeeper methods --------

class _RoundRobinSingletonPool(SingletonPool):
  """An AuroraSocketPool that creates sockets in a round-robin order
  based on a random ordering of the ZK serverset."""
  def __init__(self, *args, **kwargs):
    self._next_server = None
    super(_RoundRobinSingletonPool, self).__init__(*args, **kwargs)

  def _OnHealthyServersChanged(self):
    healhy_servers = list(self._healthy_servers)
    random.shuffle(healhy_servers)
    self._next_server = itertools.cycle(healhy_servers)

  def _GetNextServer(self):
    try:
      return self._next_server.next()
    except StopIteration:
      raise AcquireResourceException('No resources were in pool %s' %
                                     self._pool_name)

class _StickySingletonPool(SingletonPool):
  """An AuroraSocketPool that creates sockets stuck to a single instance.
  """
  def __init__(self, *args, **kwargs):
    self._random_server = None
    super(_StickySingletonPool, self).__init__(*args, **kwargs)

  def _OnHealthyServersChanged(self):
    if any(self._healthy_servers):
      self._random_server = random.choice(list(self._healthy_servers))
    else:
      self._random_server = None

  def _GetNextServer(self):
    if self._random_server:
      return self._random_server
    else:
      raise AcquireResourceException('No resources were in the pool')
