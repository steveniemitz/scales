from .base import PoolSink
from ..async import AsyncResult
from ..constants import ChannelState
from ..sink import SinkProvider


class SingletonPoolSink(PoolSink):
  """A SingletonPool maintains at most one underlying sink, and allows concurrent
  requests through it.  If the underlying sink fails, it's closed and reopened.

  This sink is intended to be used for transports that can handle concurrent
  requests, such as multiplexing transports.
  """

  def __init__(self, sink_provider, properties):
    super(SingletonPoolSink, self).__init__(sink_provider, properties)
    self._ref_count = 0

  def Open(self):
    """Open the underlying sink and increase the ref count."""
    self._ref_count += 1
    if self._ref_count > 1:
      return AsyncResult.Complete()

    def TryGet():
      self._Get()
      return True
    # We don't want to link _Get directly as it'll hold a reference
    # to the sink returned forever.
    return AsyncResult.Run(TryGet)

  def Close(self):
    """Decrease the reference count and, if zero, close the underlying transport."""
    self. _ref_count -= 1
    if self.next_sink and self._ref_count <= 0:
      sink, self.next_sink = self.next_sink, None
      sink.on_faulted.Unsubscribe(self.__PropagateShutdown)
      sink.Close()

  @property
  def state(self):
    if self.next_sink:
      return self.next_sink.state
    else:
      return ChannelState.Idle

  def __PropagateShutdown(self, value):
    self.on_faulted.Set(value)

  def _Get(self):
    if not self.next_sink:
      self.next_sink = self._sink_provider.CreateSink(self._properties)
      self.next_sink.on_faulted.Subscribe(self.__PropagateShutdown)
      self.next_sink.Open().wait()
      return self.next_sink
    elif self.next_sink.state == ChannelState.Idle:
      self.next_sink.Open().wait()
      return self.next_sink
    elif self.next_sink.is_closed:
      self.next_sink.on_faulted.Unsubscribe(self.__PropagateShutdown)
      self.next_sink = None
      return self._Get()
    else:
      return self.next_sink

  def _Release(self, sink):
    pass


SingletonPoolChannelSinkProvider = SinkProvider(SingletonPoolSink)
