from ..sink import SharedSinkProvider
from .sink import (
  KafkaRouterSink,
  KafkaSerializerSink,
  KafkaTransportSink
)
from ..core import Scales
from ..loadbalancer import HeapBalancerSink
from ..resurrector import ResurrectorSink

class _KafkaIface(object):
  def Put(self, topic, payloads=[], acks=1):
    pass

class Kafka(object):
  @staticmethod
  def _get_sink_key(properties):
    ep = properties['endpoint']
    label = properties['label']
    return ep.host, ep.port, label

  @staticmethod
  def NewBuilder():
    return Scales.NewBuilder(_KafkaIface) \
      .WithSink(KafkaRouterSink.Builder()) \
      .WithSink(HeapBalancerSink.Builder()) \
      .WithSink(KafkaSerializerSink.Builder()) \
      .WithSink(SharedSinkProvider(Kafka._get_sink_key)) \
      .WithSink(ResurrectorSink.Builder()) \
      .WithSink(KafkaTransportSink.Builder())

  @staticmethod
  def NewClient(leaders):
    return Kafka.NewBuilder() \
      .SetUri(leaders) \
      .Build()
