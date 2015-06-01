import random

from .base import (
  LoadBalancerChannelSink,
  NoMembersError
)
from ..constants import (Int, ChannelState)
from ..sink import (
  FailingChannelSink,
  ReplySink,
)


class Heap(object):
  @staticmethod
  def swap(heap, i, j):
    heap[i], heap[j] = heap[j], heap[i]
    heap[i].index = i
    heap[j].index = j

  @staticmethod
  def fixUp(heap, i):
    while i != 1 and heap[i] < heap[i/2]:
      Heap.swap(heap, i, i/2)
      i /= 2

  @staticmethod
  def fixDown(heap, i, j):
    while True:
      if j < i * 2: break

      m = 2 * i if (j == i * 2 or heap[2*i] < heap[2*i+1]) else 2*i+1
      if heap[m] < heap[i]:
        Heap.swap(heap, i, m)
      else:
        break


class ChannelReplyWrapper(ReplySink):
  __slots__ = '_putter',

  def __init__(self, wrapped, putter):
    super(ReplySink, self).__init__()
    self.next_sink = wrapped
    self._putter = putter

  def ProcessReturnMessage(self, msg):
    self._putter()
    self.next_sink.ProcessReturnMessage(msg)


class HeapBalancerChannelSink(LoadBalancerChannelSink):
  Penalty = Int.MaxValue
  Zero = Int.MinValue + 1

  class Node(object):
    __slots__ = ('_array', 'downq', 'avg_load', 'channel')

    def __init__(self, channel, load, index):
      self._array = [None, None]
      self.channel = channel
      self.avg_load = 0
      self.load = load
      self.index = index
      self.downq = None

    @property
    def load(self):
      return self._array[0]

    @load.setter
    def load(self, value):
      self._array[0] = value

    @property
    def index(self):
      return self._array[1]

    @index.setter
    def index(self, value):
      self._array[1] = value

    def __lt__(self, other):
      return self._array < other._array

  def __init__(self, next_sink_provider, service_name, server_set_provider):
    self._heap = [self.Node(FailingChannelSink(NoMembersError), self.Zero, 0)]
    self._downq = None
    self._size = 0
    super(HeapBalancerChannelSink, self).__init__(next_sink_provider, service_name, server_set_provider)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self._size == 0:
      return self._heap[0].channel
    else:
      n = self.__Get()
      n.load += 1
      Heap.fixDown(self._heap, n.index, self._size)
      self._OnGet(n)
      put_called = [False]
      def PutWrapper():
        if not put_called[0]:
          put_called[0] = True
          self.__Put(n)

      reply_sink = ChannelReplyWrapper(sink_stack.reply_sink, PutWrapper)
      sink_stack.reply_sink = reply_sink
      sink_stack.Push(self, PutWrapper)
      n.channel.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    context()
    sink_stack.AsyncProcessResponse(stream, msg)

  def __Put(self, n):
    n.load -= 1
    if n.load < self.Zero:
      self.LOG.warning('Decrementing load below Zero')
    if n.index < 0:
      pass
    elif n.load == self.Zero and self._size > 1:
      i = n.index
      Heap.swap(self._heap, i, self._size)
      Heap.fixDown(self._heap, i, self._size - 1)

      j = random.randint(1, self._size)
      Heap.swap(self._heap, j, self._size)
      Heap.fixUp(self._heap, j)

      Heap.fixUp(self._heap, self._size)
    else:
      Heap.fixUp(self._heap, n.index)
    self._OnPut(n)

# Events overridable by subclasses
  def _OnNodeDown(self, node):
    pass

  def _OnNodeUp(self, node):
    pass

  def _OnGet(self, node):
    pass

  def _OnPut(self, node):
    pass

  def __Get(self):
    while True:
      n = self._downq
      m = None
      while n is not None:
        if n.index < 0:
          n = n.downq
          if m is None:
            self._downq = n
          else:
            m.downq = n
        elif n.channel.state <= ChannelState.Open:
          n.load -= self.Penalty
          Heap.fixUp(self._heap, n.index)
          o = n.downq
          n.downq = None
          n = o
          if m is None:
            self._downq = n
          else:
            m.downq = n
        else:
          m, n = n, n.downq

      n = self._heap[1]
      if n.channel.state <= ChannelState.Open or n.load >= 0:
        return n
      else:
        self.LOG.warning('Marking node %s down' % n)
        # Node is now down
        n.downq = self._downq
        self._downq = n
        n.load += self.Penalty
        Heap.fixDown(self._heap, 1, self._size)
        self._OnNodeDown(n)
        # Loop

  def _AddNode(self, channel):
    channel.Open()
    self._size += 1
    new_node = self.Node(channel, self.Zero, self._size)
    self._heap.append(new_node)
    Heap.fixUp(self._heap, self._size)

  def _RemoveNode(self, channel):
    i = next(idx for idx, node in enumerate(self._heap) if node.channel == channel)
    node = self._heap[i]
    Heap.swap(self._heap, i, self._size)
    Heap.fixDown(self._heap, i, self._size - 1)
    self._heap.pop()
    self._size -= 1
    node.index = -1
    node.channel.Close()

  def _OnServersChanged(self, endpoint, added):
    _, channel = endpoint
    if added:
      self._AddNode(channel)
    else:
      self._RemoveNode(channel)

  def Open(self):
    pass

  def Close(self):
    [n.channel.Close() for n in self._heap]

  @property
  def state(self):
    pass
