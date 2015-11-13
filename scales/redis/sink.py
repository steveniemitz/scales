import time

import gevent
import redis

from ..async import (
  AsyncResult,
  NoopTimeout
)
from ..sink import (
  ClientMessageSink,
  SinkProvider
)
from ..constants import (
  ChannelState,
  SinkProperties,
)
from ..message import (
  ChannelConcurrencyError,
  Deadline,
  MethodReturnMessage,
  TimeoutError
)
from ..varz import (
  AverageTimer,
  Rate,
  Source,
  VarzBase,
)

class RedisTransportSink(ClientMessageSink):
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.redis.RedisTransportSink'
    _VARZ = {
      'messages_sent': Rate,
      'messages_recv': Rate,
      'transport_latency': AverageTimer
    }

  def __init__(self, next_provider, sink_properties, global_properties):
    super(RedisTransportSink, self).__init__()
    self._endpoint = global_properties[SinkProperties.Endpoint]
    self._state = ChannelState.Idle
    name = global_properties[SinkProperties.Label]
    socket_source = '%s:%d' % (self._endpoint.host, self._endpoint.port)
    self._varz = self.Varz(Source(service=name, endpoint=socket_source))
    self._open_result = None
    self._client = None
    self._processing = None

  def Open(self):
    if not self._open_result:
      self._open_result = AsyncResult()
      self._open_result.SafeLink(self._OpenImpl)
    return self._open_result

  def _OpenImpl(self):
    self._client = redis.StrictRedis(
        host=self._endpoint.host,
        port=self._endpoint.port)
    try:
      self._client.ping()
      self._state = ChannelState.Open
    except Exception as ex:
      self._Fault(ex)
      raise

  def Close(self):
    self._state = ChannelState.Closed
    if self._client:
      client, self._client = self._client, None
      client.connection_pool.disconnect()
    self._open_result = None
    if self._processing:
      p, self._processing = self._processing, None
      p.kill(block=False)

  def _Fault(self, reason):
    if self.state == ChannelState.Closed:
      return

    self.Close()
    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    self._on_faulted.Set(reason)

  @property
  def state(self):
    return self._state

  def _AsyncProcessTransaction(self, sink_stack, msg, deadline):
    method = getattr(self._client, msg.method)
    with self._varz.transport_latency.Measure():
      gtimeout = None
      try:
        if deadline:
          timeout = deadline - time.time()
          if timeout < 0:
            raise gevent.Timeout()
          gtimeout = gevent.Timeout.start_new(timeout)
        else:
          gtimeout = NoopTimeout()

        self._varz.messages_sent()
        ret = method(*msg.args, **msg.kwargs)
        msg = MethodReturnMessage(ret)
        gtimeout.cancel()
        self._processing = None
        gevent.spawn(sink_stack.AsyncProcessResponseMessage, msg)
      except gevent.Timeout: # pylint: disable=E0712
        err = TimeoutError()
        self._client.connection_pool.disconnect()
        self._processing = None
        gevent.spawn(sink_stack.AsyncProcessResponseMessage, MethodReturnMessage(error=err))
      except Exception as err:
        self._processing = None
        if gtimeout:
          gtimeout.cancel()
        self._Fault(err)
        gevent.spawn(sink_stack.AsyncProcessResponseMessage, MethodReturnMessage(error=err))
      finally:
        self._varz.messages_recv()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self._processing:
      sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(
        error=ChannelConcurrencyError(
          'Concurrency violation in AsyncProcessRequest')))
      return
    deadline = msg.properties.get(Deadline.KEY)
    self._processing = gevent.spawn(self._AsyncProcessTransaction, sink_stack, msg, deadline)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass

RedisTransportSink.Builder = SinkProvider(RedisTransportSink)
