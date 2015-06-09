import logging
import time

import gevent

from .constants import (ChannelState, SinkProperties)
from .message import (FailedFastError, MethodReturnMessage)
from .sink import (ClientMessageSink, SinkProvider)
from .varz import (AggregateTimer, Counter, SourceType, VarzBase)

ROOT_LOG = logging.getLogger('scales.Resurrector')

class ResurrectorSink(ClientMessageSink):
  """The resurrector sink monitors its underlying sink for faults, and begins
   attempting to resurrect it.

   Additionally, it will fail any incoming requests immediately while the
   underlying sink is failed.
   """

  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.Resurrector'
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'time_failed': AggregateTimer,
      'reconnect_attempts': Counter,
    }

  def __init__(self, next_factory, properties):
    endpoint = properties[SinkProperties.Endpoint]
    service = properties[SinkProperties.Service]
    endpoint_source = '%s:%d' % (endpoint.host, endpoint.port)
    self._log = ROOT_LOG.getChild('[%s.%s]' % (service, endpoint_source))
    self._properties = properties
    self._next_factory = next_factory
    self._resurrector = None
    self._down_on = None
    self._varz = self.Varz((service, endpoint_source))
    super(ResurrectorSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self.next_sink is None:
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=FailedFastError()))
    else:
      self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called")

  def _OnSinkClosed(self, val):
    if not self._down_on:
      self._down_on = time.time()
      sink, self.next_sink = self.next_sink, None
      sink.Close()
      sink.on_faulted.Unsubscribe(self._OnSinkClosed)
      self._resurrector = gevent.spawn(self._TryResurrect)
    self.on_faulted.Set(val)

  def _TryResurrect(self):
    last_attempt = self._down_on
    wait_interval = 5
    self._log.info('Attempting to reopen faulted channel')
    while True:
      gevent.sleep(wait_interval)
      down_time = time.time() - last_attempt
      last_attempt = time.time()
      self._varz.time_failed(down_time)

      sink = self._next_factory.CreateSink(self._properties)
      try:
        self._varz.reconnect_attempts()
        sink.Open().get()
        sink.on_faulted.Subscribe(self._OnSinkClosed)
        self.next_sink = sink
        self._down_on = None
        self._log.info('Reopened channel.')
        return
      except:
        sink.Close()

      wait_interval **= 1.2
      if wait_interval > 60:
        wait_interval = 60

  def Open(self):
    if not self.next_sink:
      self.next_sink = self._next_factory.CreateSink(self._properties)
      self.next_sink.on_faulted.Subscribe(self._OnSinkClosed)
    return self.next_sink.Open()

  def Close(self):
    if self._resurrector:
      self._resurrector.kill()
    self._down_on = None
    if self.next_sink:
      self.next_sink.on_faulted.Unsubscribe(self._OnSinkClosed)
      self.next_sink.Close()

  @property
  def state(self):
    if self._down_on:
      return ChannelState.Closed
    elif not self.next_sink:
      return ChannelState.Idle
    else:
      return self.next_sink.state

ResurrectorSinkProvider = SinkProvider(ResurrectorSink)
