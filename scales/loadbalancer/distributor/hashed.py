from ...constants import SinkRole
from ...async import AsyncResult
from ...sink import (
  FailingMessageSink,
  SinkProvider
)
from .base import DistributorSink

import logging
logging.basicConfig(level='DEBUG')

class HashedDistributorSink(DistributorSink):
  def __init__(self, next_provider, sink_properties, global_properties):
    self._hasher = sink_properties.hasher
    self._buckets = {}
    self._no_bucket_sink = FailingMessageSink(lambda: Exception('No bucket existed for hash'))
    self._open = False
    super(HashedDistributorSink, self).__init__(next_provider, sink_properties, global_properties)

  def _CreateLoadBalancer(self):
    server_set = self.ChildServerSetProvider()
    lbb = self._next_sink_provider
    new_lbb = lbb.Clone(server_set_provider=server_set)
    lb = new_lbb.CreateSink(self._properties)
    if self._open:
      lb.Open()
    return server_set, lb

  def __OnServerAdded(self, instance, bucket_hash, bucket_info):
    if not bucket_info:
      self._log.info('Creating load balancer for shard %s', str(bucket_hash))
      server_set, lb = self._CreateLoadBalancer()
      self._buckets[bucket_hash] = (server_set, lb)
    else:
      server_set, lb = bucket_info
    server_set.AddServer(instance)

  def __OnServerRemoved(self, instance, bucket_info):
    server_set, _ = bucket_info
    server_set.RemoveServer(instance)

  def _OnServersChanged(self, instance, channel_factory, added):
    bucket = self._hasher.HashEndpoint(instance)
    bucket_info = self._buckets.get(bucket, None)
    if added:
      self.__OnServerAdded(instance, bucket, bucket_info)
    elif bucket_info:
      self.__OnServerRemoved(instance, bucket_info)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    bucket = self._hasher.HashMessage(msg)
    bucket_info = self._buckets.get(bucket, None)
    if not bucket_info:
      sink = self._no_bucket_sink
    else:
      _, sink = bucket_info
      if not sink.is_open:
        sink.Open().wait()
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    # Never called
    pass

  def Open(self):
    self._open = True
    if any(self._buckets):
      return AsyncResult.WhenAny([b[1].Open()
                                  for b in self._buckets.values()])
    else:
      return AsyncResult.Complete()

HashedDistributorSink.Builder = SinkProvider(
  HashedDistributorSink,
  SinkRole.LoadBalancer,
  hasher=None,
  server_set_provider=None
)
