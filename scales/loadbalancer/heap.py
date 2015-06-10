"""Heap Load Balancer.

Based on the heap balancer from finagle, see (https://github.com/twitter/finagle/blob/master/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/HeapBalancer.scala)

The heap load balancer maintains all nodes in a min-heap.  The heap is adjusted
as the load on a node increases or decreases.  Nodes start at Zero (min int) and
increase load as they are used.  Load decreases when they are released back into
the pool.

Downed nodes are tracked by setting a node's load to > 0.  A linked list of downed
nodes is kept to resurrect downed nodes if they become active again.
"""

import random

from .base import (
  LoadBalancerSink,
  NoMembersError
)
from ..async import AsyncResult
from ..constants import (Int, ChannelState)
from ..sink import (
  FailingMessageSink,
)


class Heap(object):
  """A utility class to perform heap functions"""
  @staticmethod
  def Swap(heap, i, j):
    """Swap two elements in the heap.

    Args:
      heap - The heap array.
      i, j - The indexes int the array to swap.
    """
    heap[i], heap[j] = heap[j], heap[i]
    heap[i].index = i
    heap[j].index = j

  @staticmethod
  def FixUp(heap, i):
    """Traverse up the heap, ensuring the invariant is maintained.

    Args:
      heap - The heap array.
      i - The index to start at.
    """
    while i != 1 and heap[i] < heap[i/2]:
      Heap.Swap(heap, i, i/2)
      i /= 2

  @staticmethod
  def FixDown(heap, i, j):
    """Traverse down the heap, ensuring the invariant is maintained.

    Args:
      heap - The heap array.
      i, j - The node index to traverse from -> to.
    """
    while True:
      if j < i * 2: break

      m = 2 * i if (j == i * 2 or heap[2*i] < heap[2*i+1]) else 2*i+1
      if heap[m] < heap[i]:
        Heap.Swap(heap, i, m)
      else:
        break


class HeapBalancerSink(LoadBalancerSink):
  """A sink that implements a heap load balancer."""
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
      """Compare to other, return true if (load, index) < other.(load, index)"""
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
      n = self._heap[0]
    else:
      n = self.__Get()
      n.load += 1
      Heap.FixDown(self._heap, n.index, self._size)
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

  # Events override by subclasses
  def _OnNodeDown(self, node):
    pass

  def _OnNodeUp(self, node):
    pass

  def _OnGet(self, node):
    pass

  def _OnPut(self, node):
    pass

  def __Get(self):
    """Get the least-loaded node from the heap.

    Returns:
      A node.
    """
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
        elif n.channel.state == ChannelState.Open:
          # The node was resurrected, mark it back up
          self._log.info('Marking node %s up' % n)
          n.load -= self.Penalty
          Heap.FixUp(self._heap, n.index)
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
      if n.channel.state == ChannelState.Open:
        return n
      elif n.load >= 0:
        if n.channel.state  == ChannelState.Idle:
          # This node was the last chance, but still idle.
          # At this point there's nothing to do but wait for it to open.
          n.channel.Open().wait()
        else:
          return n
      else:
        if n.channel.state == ChannelState.Closed:
          self._log.warning('Marking node %s down' % n)
        # Node is now down
        n.downq = self._downq
        self._downq = n
        n.load += self.Penalty
        Heap.FixDown(self._heap, 1, self._size)
        if n.channel.state == ChannelState.Closed:
          self._OnNodeDown(n)
        # Loop

  def __Put(self, n):
    """'Return' a member to the heap.  Load on the node is decremented and its
    position in the heap is adjusted.

    Args:
      n - The node to return
    """
    n.load -= 1
    if n.load < self.Zero:
      self._log.warning('Decrementing load below Zero')
    if n.index < 0 and n.load > self.Zero:
      pass
    elif n.index < 0 and n.load == self.Zero:
      n.channel.Close()
    elif n.load == self.Zero and self._size > 1:
      i = n.index
      Heap.Swap(self._heap, i, self._size)
      Heap.FixDown(self._heap, i, self._size - 1)

      j = random.randint(1, self._size)
      Heap.Swap(self._heap, j, self._size)
      Heap.FixUp(self._heap, j)

      Heap.FixUp(self._heap, self._size)
    else:
      Heap.FixUp(self._heap, n.index)
    self._OnPut(n)

  def _AddNode(self, sink):
    """Add a sink to the heap.
    The sink is immediately opened and initialized to Zero load.

    Args:
      sink - The sink that was just added.
    """
    # It's ok if Open() threw an exception here, it'll be detected in __Get
    # and the node marked down.  We still want downed nodes in the heap.
    self._size += 1
    new_node = self.Node(sink, self.Zero, self._size)
    self._heap.append(new_node)
    Heap.FixUp(self._heap, self._size)
    # Adding an Open() in here allows us to optimistically assume it'll be opened
    # before the next message attempts to get it.  However, the Open() will likely
    # yield, so other code paths need to be aware there is a potentially un-open
    # sink on the heap.
    sink.Open().wait()

  def _RemoveNode(self, sink):
    """Remove a sink from the heap.
    The sink is closed immediately if it has no outstanding load, otherwise the
    close is deferred until the sink goes idle.

    Args:
      sink - The sink to be removed.
    """
    i = next((idx for idx, node in enumerate(self._heap) if node.channel == sink), 0)
    # The sink has already been removed from the heap.
    if i == 0:
      return
    node = self._heap[i]
    Heap.Swap(self._heap, i, self._size)
    Heap.FixDown(self._heap, i, self._size - 1)
    self._heap.pop()
    self._size -= 1
    node.index = -1
    if node.load == self.Zero:
      node.channel.Close()

  def _OnServersChanged(self, endpoint, added):
    """Invoked by the LoadBalancer when an endpoint joins or leaves the
    server set.

    Args:
      endpoint - A tuple of (endpoint, sink).
      added - True if the endpoint is being added, False if being removed.
    """
    _, channel = endpoint
    if added:
      self._AddNode(channel)
    else:
      self._RemoveNode(channel)

  def Open(self):
    """Open the sink and all underlying nodes."""
    if self._size > 0:
      # Ignore the first sink, it's the FailingChannelSink.
      return AsyncResult.WhenAny([n.channel.Open() for n in self._heap[1:]])
    else:
      return AsyncResult.Complete()

  def Close(self):
    """Close the sink and all underlying nodes immediately."""
    [n.channel.Close() for n in self._heap]

  @property
  def state(self):
    return max([n.channel.state for n in self._heap])
