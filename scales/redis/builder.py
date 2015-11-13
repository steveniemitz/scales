
from ..loadbalancer import ApertureBalancerSink
from ..pool import WatermarkPoolSink
from ..core import Scales

from .sink import RedisTransportSink

class Redis(object):
  @staticmethod
  def NewBuilder():
    import redis
    return Scales.NewBuilder(redis.StrictRedis) \
      .WithSink(ApertureBalancerSink.Builder()) \
      .WithSink(WatermarkPoolSink.Builder()) \
      .WithSink(RedisTransportSink.Builder())

  @staticmethod
  def NewClient(uri):
    return Redis.NewBuilder() \
      .SetUri(uri) \
      .Build()
