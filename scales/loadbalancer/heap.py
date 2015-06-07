import random

from .base import (
  LoadBalancerSink,
  NoMembersError
)
from ..constants import (Int, ChannelState)
from ..sink import (
  FailingMessageSink,
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


class HeapBalancerSink(LoadBalancerSink):
  Penalty = Int.MaxValue
  Zero = Int.MinValue + 1

  class Node(object):
    __slots__ = ('load', 'index', 'downq', 'avg_load', 'channel')

    def __init__(self, channel, load, index):
      self.channel = channel
      self.avg_load = 0
      self.load = load
      self.index = index
      self.downq = None

    def __lt__(self, other):
      if self.load < other.load:
        return True
      else:
        return self.index < other.index

  def __init__(self, next_provider, properties):
    self._heap = [self.Node(FailingMessageSink(NoMembersError), self.Zero, 0)]
    self._downq = None
    self._size = 0
    super(HeapBalancerSink, self).__init__(next_provider, properties)

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

      sink_stack.Push(self, PutWrapper)
      n.channel.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    context()
    sink_stack.AsyncProcessResponse(stream, msg)

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
          # The node has been discarded.
          n = n.downq
          if m is None:
            self._downq = n
          else:
            m.downq = n
        elif n.channel.state <= ChannelState.Open:
          # The node was resurrected, mark it back up
          self._log.info('Marking node %s up' % n)
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
          # No change, move to the next node in the linked list
          m, n = n, n.downq

      n = self._heap[1]
      if n.channel.state <= ChannelState.Open or n.load >= 0:
        return n
      else:
        self._log.warning('Marking node %s down' % n)
        # Node is now down
        n.downq = self._downq
        self._downq = n
        n.load += self.Penalty
        Heap.fixDown(self._heap, 1, self._size)
        self._OnNodeDown(n)
        # Loop

  def __Put(self, n):
    n.load -= 1
    if n.load < self.Zero:
      self._log.warning('Decrementing load below Zero')
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

  def Open(self, force=False):
    pass

  def Close(self):
    [n.channel.Close() for n in self._heap]

  @property
  def state(self):
    return max([n.channel.state for n in self._heap])
