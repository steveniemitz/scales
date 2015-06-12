from .sink import (
  HttpTransportSinkProvider
)
from ..builder import BaseBuilder
from ..core import Scales
from ..loadbalancer.aperture import ApertureBalancerSinkProvider
from ..pool import SingletonPoolChannelSinkProvider


class _HttpIface(object):
  def Get(self, url, **kwargs):
    pass

  def Post(self, url, **kwargs):
    pass


class Http(object):
  DEFAULT_TIMEOUT = 60

  class SinkProvider(BaseBuilder.SinkProviderProvider):
    _PROVIDERS = [
      ApertureBalancerSinkProvider,
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
