"""An abstract base class for load balancer.
Load balancers track an underlying server set, and return a sink to service a
request.
"""

import logging

from gevent.event import Event

from ..constants import SinkProperties
from ..sink import ClientMessageSink

ROOT_LOG = logging.getLogger("scales.loadbalancer")

class NoMembersError(Exception): pass

class LoadBalancerSink(ClientMessageSink):
  """Base class for all load balancer sinks."""
  def __init__(self, next_provider, properties):
    self._properties = properties
    service_name = properties[SinkProperties.Service]
    server_set_provider = properties['server_set_provider']
    log_name = self.__class__.__name__.replace('ChannelSink', '')
    self._log = ROOT_LOG.getChild('%s.[%s]' % (log_name, service_name))
    self._init_done = Event()
    self._server_set_provider = server_set_provider
    self._next_sink_provider = next_provider
    server_set_provider.Initialize(
      self._OnServerSetJoin,
      self._OnServerSetLeave)
    server_set = server_set_provider.GetServers()
    self._servers = {}
    [self._AddServer(m) for m in server_set]
    self._init_done.set()
    super(LoadBalancerSink, self).__init__()

  def _OnServersChanged(self, instance, added):
    """Overridable by child classes.  Invoked when servers in the server set are
    added or removed.

    Args:
      instance - The server set member being added or removed.
      added - True if the instance is being added, False if it's being removed.
    """
    pass

  def _AddServer(self, instance):
    """Adds a servers to the set of servers available to the load balancer.
    Note: The new sink is not opened at this time.

    Args:
      instance - A Member object to be added to the pool.
    """
    if not instance.service_endpoint in self._servers:
      new_props = self._properties.copy()
      new_props.update({ SinkProperties.Endpoint: instance.service_endpoint })
      channel = self._next_sink_provider.CreateSink(new_props)
      self._servers[instance.service_endpoint] = channel
      self._log.info("Instance %s joined (%d members)" % (instance.service_endpoint, len(self._servers)))
      self._OnServersChanged((instance, channel), True)

  def _RemoveServer(self, instance):
    """Removes a server from the load balancer.

    Args:
      instance - A Member object to be removed from the pool.
    """
    channel = self._servers.pop(instance.service_endpoint, None)
    self._OnServersChanged((instance, channel), False)

  def _OnServerSetJoin(self, instance):
    """Invoked when an instance joins the server set.

    Args:
      instance - Instance added to the cluster.
    """
    # callbacks from the ServerSet are delivered serially, so we can guarantee
    # that once this unblocks, we'll still get the notifications delivered in
    # the order that they arrived.  Ex: OnJoin(a) -> OnLeave(a)
    self._init_done.wait()
    # OnJoin notifications are delivered at startup, however we already
    # pre-populate our copy of the ServerSet, so it's fine to ignore duplicates.
    if instance.service_endpoint in self._servers:
      return

    self._AddServer(instance)

  def _OnServerSetLeave(self, instance):
    """Invoked when an instance leaves the server set.

    Args:
      instance - Instance leaving the cluster.
    """
    self._init_done.wait()
    self._RemoveServer(instance)

    self._log.info("Instance left (%d members)" % len(self._servers))
