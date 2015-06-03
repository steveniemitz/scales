import logging

from gevent.event import Event

from ..sink import ClientChannelSink

ROOT_LOG = logging.getLogger("scales.loadbalancer")

class NoMembersError(Exception): pass

class LoadBalancerChannelSink(ClientChannelSink):
  def __init__(self, next_sink_provider, service_name, server_set_provider):
    log_name = self.__class__.__name__.replace('ChannelSink', '')
    self._log = ROOT_LOG.getChild('%s.[%s]' % (log_name, service_name))
    self._init_done = Event()
    self._service_name = service_name
    self._server_set_provider = server_set_provider
    self._next_sink_provider = next_sink_provider
    server_set_provider.Initialize(
      self._OnServerSetJoin,
      self._OnServerSetLeave)
    server_set = server_set_provider.GetServers()
    self._servers = {}
    [self._AddServer(m) for m in server_set]
    self._init_done.set()
    super(LoadBalancerChannelSink, self).__init__()

  def _OnServersChanged(self, instance, added):
    pass

  def _AddServer(self, instance):
    """Adds a servers to the set of servers available to the connection pool.
    Note: no new connections are created at this time.

    Args:
      instance - A Member object to be added to the pool.
    """
    if not instance.service_endpoint in self._servers:
      channel = self._next_sink_provider.CreateSink(
          instance.service_endpoint,
          self._service_name)
      self._servers[instance.service_endpoint] = channel
      self._log.info("Instance %s joined (%d members)" % (instance.service_endpoint, len(self._servers)))
      self._OnServersChanged((instance, channel), True)

  def _RemoveServer(self, instance):
    """Removes a server from the connection pool.  Outstanding connections will
    be closed lazily.

    Args:
      instance - A Member object to be removed from the pool.
    """
    channel = self._servers.pop(instance.service_endpoint, None)
    self._OnServersChanged((instance, channel), False)

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

  def _OnServerSetLeave(self, instance):
    """Invoked when an instance leaves the cluster.

    If the instance leaving the cluster is the chosen shard,
    then the connections will be reset.

    Args:
      instance - Instance leaving the cluster.
    """
    self._init_done.wait()
    self._RemoveServer(instance)

    self._log.info("Instance left (%d members)" % len(self._servers))

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise Exception("This should never be called.")
