import logging

from gevent.event import Event
import gevent

from .constants import ChannelState
from .sink import ClientChannelSink

ROOT_LOG = logging.getLogger('scales.Resurrector')

class ResurrectorChannelSink(ClientChannelSink):
  def __init__(self, next_factory, endpoint, name):
    self.LOG = ROOT_LOG.getChild('[%s.%s:%s]' % (name, endpoint.host, endpoint.port))
    self._endpoint = endpoint
    self._name = name
    self._next_factory = next_factory
    self._close_event = Event()
    self._resurrecting = False
    super(ResurrectorChannelSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called")

  def _OnSinkClosed(self, val):
    if not self._resurrecting:
      self._resurrecting = True
      sink, self.next_sink = self.next_sink, None
      sink.Close()
      sink.on_faulted.Unsubscribe(self._OnSinkClosed)

      gevent.spawn(self._TryResurrect)
    self.on_faulted.Set(val)

  def _TryResurrect(self):
    wait_interval = 5
    self.LOG.info('Attempting to reopen faulted channel')
    while True:
      if self._close_event.wait(wait_interval):
        # The channel was closed, exit the loop
        return

      sink = self._next_factory.CreateSink(self._endpoint, self._name)
      try:
        sink.Open(True)
        sink.on_faulted.Subscribe(self._OnSinkClosed)
        self.next_sink = sink
        self._resurrecting = False
        self.LOG.info('Reopened channel.')
        return
      except:
        sink.Close()

      wait_interval **= 1.2
      if wait_interval > 60:
        wait_interval = 60

  def Open(self, force=False):
    if not self.next_sink:
      self.next_sink = self._next_factory.CreateSink(self._endpoint, self._name)
      self.next_sink.on_faulted.Subscribe(self._OnSinkClosed)

  def Close(self):
    self._close_event.set()
    self.next_sink.on_faulted.Unsubscribe(self._OnSinkClosed)
    self.next_sink.Close()

  @property
  def state(self):
    if self._resurrecting:
      return ChannelState.Closed
    elif not self.next_sink:
      return ChannelState.Idle
    else:
      return self.next_sink.state


class ResurrectorChannelSinkProvider(object):
  def __init__(self, next_provider):
    self._next_provider = next_provider

  def CreateSink(self, endpoint, name):
    return ResurrectorChannelSink(self._next_provider, endpoint, name)
