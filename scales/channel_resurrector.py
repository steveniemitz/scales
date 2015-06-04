import logging

import gevent

from .constants import ChannelState
from .future import Future

ROOT_LOG = logging.getLogger('scales.Resurrector')

class ConnectionResurrector(object):
  def __init__(self, next_factory, endpoint, name):
    self._log = ROOT_LOG.getChild('[%s.%s:%s]' % (name, endpoint.host, endpoint.port))
    self._endpoint = endpoint
    self._name = name
    self._next_factory = next_factory
    self._resurrector = None
    self._resurrecting = False
    self._next = None
    super(ConnectionResurrector, self).__init__()

  def __call__(self, *args, **kwargs):
    return self._next(*args, **kwargs)

  def _OnSinkClosed(self, val):
    if not self._resurrecting:
      self._resurrecting = True
      sink, self._next = self._next, None
      sink.Close()
      sink.on_faulted.Unsubscribe(self._OnSinkClosed)
      self._resurrector = gevent.spawn(self._TryResurrect)
    self.on_faulted.Set(val)

  def _TryResurrect(self):
    wait_interval = 5
    self._log.info('Attempting to reopen faulted channel')
    while True:
      sink = self._next_factory.CreateSink(self._endpoint, self._name)
      try:
        sink.Open(True)
        sink.on_faulted.Subscribe(self._OnSinkClosed)
        self._next = sink
        self._resurrecting = False
        self._log.info('Reopened channel.')
        return
      except:
        sink.Close()

      wait_interval **= 1.2
      if wait_interval > 60:
        wait_interval = 60

  def Open(self, force=False):
    if not self._next:
      self._next = self._next_factory.CreateSink(self._endpoint, self._name)
      self._next.on_faulted.Subscribe(self._OnSinkClosed)
    return Future.FromResult(True)

  def Close(self):
    if self._resurrector:
      self._resurrector.kill()
    self._next.on_faulted.Unsubscribe(self._OnSinkClosed)
    return self._next.Close()

  @property
  def state(self):
    if self._resurrecting:
      return ChannelState.Closed
    elif not self._next:
      return ChannelState.Idle
    else:
      return self._next.state


class ResurrectorChannelSinkProvider(object):
  def __init__(self, next_provider):
    self._next_provider = next_provider

  def CreateSink(self, endpoint, name):
    return ConnectionResurrector(self._next_provider, endpoint, name)
