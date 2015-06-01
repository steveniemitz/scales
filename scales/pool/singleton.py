from ..constants import ChannelState
from ..sink import ChannelSinkProvider
from .base import PoolChannelSink


class SingletonPoolChannelSink(PoolChannelSink):
  def __init__(self, sink_provider, endpoint, name, properties):
    self._ref_count = 0
    super(SingletonPoolChannelSink, self).__init__(sink_provider, endpoint, name, properties)

  def Open(self, force=False):
    self._ref_count += 1
    if force:
      self._Get()

  def Close(self):
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
      self.next_sink = self._sink_provider.CreateSink(self._endpoint, self._name, None)
      self.next_sink.on_faulted.Subscribe(self.__PropagateShutdown)
      self.next_sink.Open()
      return self.next_sink
    elif self.next_sink.state > ChannelState.Open:
      self.next_sink.on_faulted.Unsubscribe(self.__PropagateShutdown)
      self.next_sink = None
      return self._Get()
    else:
      return self.next_sink

  def _Release(self, sink):
    pass


SingletonPoolChannelSinkProvider = ChannelSinkProvider(SingletonPoolChannelSink)
