from .sink import (
  HttpTransportSinkProvider
)
from ..builder import BaseBuilder
from ..core import Scales
from ..loadbalancer.aperture import ApertureBalancerChannelSinkProvider
from ..pool import SingletonPoolChannelSinkProvider


class _HttpIface(object):
  def Get(self, url, args):
    pass

  def Post(self, url, args):
    pass


class Http(object):
  DEFAULT_TIMEOUT = 60

  class SinkProvider(BaseBuilder.SinkProvider):
    _PROVIDERS = [
      ApertureBalancerChannelSinkProvider,
      SingletonPoolChannelSinkProvider,
      HttpTransportSinkProvider
    ]

  @staticmethod
  def Configure():
    return Scales.NewBuilder(_HttpIface)\
      .SetSinkProvider(Http.SinkProvider())

  @staticmethod
  def NewClient(uri, timeout=DEFAULT_TIMEOUT):
    return Http.Configure()\
      .SetUri(uri)\
      .SetTimeout(timeout)\
      .Build()
