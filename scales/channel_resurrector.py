import logging

import gevent

from . import async_util
from .constants import (ChannelState, SinkProperties)
from .message import (FailedFastError, MethodReturnMessage)
from .sink import (ClientMessageSink, ChannelSinkProvider)

ROOT_LOG = logging.getLogger('scales.Resurrector')

class ResurrectorSink(ClientMessageSink):
  def __init__(self, next_factory, properties):
    endpoint = properties[SinkProperties.Endpoint]
    service = properties[SinkProperties.Service]
    self._log = ROOT_LOG.getChild('[%s.%s:%s]' % (service, endpoint.host, endpoint.port))
    self._properties = properties
    self._next_factory = next_factory
    self._resurrecting = False
    self._resurrector = None
    super(ResurrectorSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self.next_sink is None:
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=FailedFastError()))
    else:
      self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called")

  def _OnSinkClosed(self, val):
    if not self._resurrecting:
      self._resurrecting = True
      sink, self.next_sink = self.next_sink, None
      sink.Close()
      sink.on_faulted.Unsubscribe(self._OnSinkClosed)
      self._resurrector = gevent.spawn(self._TryResurrect)
    self.on_faulted.Set(val)

  def _TryResurrect(self):
    wait_interval = 5
    self._log.info('Attempting to reopen faulted channel')
    while True:
      gevent.sleep(wait_interval)
      sink = self._next_factory.CreateSink(self._properties)
      try:
        sink.Open(True)
        sink.on_faulted.Subscribe(self._OnSinkClosed)
        self.next_sink = sink
        self._resurrecting = False
        self._log.info('Reopened channel.')
        return
      except:
        sink.Close()

      wait_interval **= 1.2
      if wait_interval > 60:
        wait_interval = 60

  def Open(self, force=False):
    if not self.next_sink:
      self.next_sink = self._next_factory.CreateSink(self._properties)
      self.next_sink.on_faulted.Subscribe(self._OnSinkClosed)
    return self.next_sink.Open()

  def Close(self):
    if self._resurrector:
      self._resurrector.kill()
    self._resurrecting = False
    if self.next_sink:
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

ResurrectorChannelSinkProvider = ChannelSinkProvider(ResurrectorSink)
