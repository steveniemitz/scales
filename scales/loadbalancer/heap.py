"""Heap Load Balancer.

Based on the heap balancer from finagle, see (https://github.com/twitter/finagle/blob/master/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/HeapBalancer.scala)

The heap load balancer maintains all nodes in a min-heap.  The heap is adjusted
as the load on a node increases or decreases.  Nodes start at Zero (min int) and
increase load as they are used.  Load decreases when they are released back into
the pool.

Downed nodes are tracked by setting a node's load to > 0.  A linked list of downed
nodes is kept to resurrect downed nodes if they become active again.
"""

import functools
import random

try:
  from gevent.lock import RLock # pylint: disable=E0611
except ImportError:
  from gevent.coros import RLock

from .base import (
  LoadBalancerSink,
  NoMembersError
)
from ..async import AsyncResult
from ..constants import (
  ChannelState,
  Int,
  MessageProperties,
  SinkProperties
)
from ..sink import FailingMessageSink
from ..varz import (
  Counter,
  Gauge,
  Source,
  VarzBase
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
    if i == 0 or j == 0:
      raise Exception("heap swapping of element 0 should never happen.")

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
    while True:
      if i != 1 and heap[i] < heap[i/2]:
        Heap.Swap(heap, i, i/2)
        i /= 2 # FixUp(heap, i/2)
      else:
        break

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
        i = m  # FixDown(heap, m, j)
      else:
        break


def synchronized(fn):
  """Runs the wrapped method under a lock.
  The self parameter to the wrapped function is expected to have a __heap_lock
  attribute.
  """
  @functools.wraps(fn)
  def wrapper(self, *args, **kwargs):
    with self._heap_lock:
      return fn(self, *args, **kwargs)
  return wrapper


class HeapBalancerSink(LoadBalancerSink):
  """A sink that implements a heap load balancer."""
  Penalty = Int.MaxValue
  Idle = Int.MinValue + 1

  class HeapVarz(VarzBase):
    """
    size - The number of nodes in the pool
    no_members - The number of times the balancer served a failing requests
                 because there were no members in the pool.
    """
    _VARZ_BASE_NAME = 'scales.loadbalancer.Heap'
    _VARZ = {
      'size': Gauge,
      'no_members': Counter
    }

  class Node(object):
    __slots__ = ('load', 'index', 'downq', 'avg_load', 'channel', 'endpoint')

    def __init__(self, channel, load, index, endpoint):
      self.channel = channel
      self.avg_load = 0
      self.load = load
      self.index = index
      self.downq = None
      self.endpoint = endpoint

    def __lt__(self, other):
      """Compare to other, return true if (load, index) < other.(load, index)"""
      if self.load > other.load:
        return False
      elif self.load < other.load:
        return True
      else:
        return self.index < other.index

  def __init__(self, next_provider, sink_properties, global_properties):
    self._heap = [self.Node(
        FailingMessageSink(functools.partial(Exception, "this sink should never be used")),
        self.Idle, 0, None)]
    self._no_members = FailingMessageSink(NoMembersError)
    self._downq = None
    self._size = 0
    self._open = False
    self._heap_lock = RLock()
    service_name = global_properties[SinkProperties.Label]
    self.__varz = self.HeapVarz(Source(service=service_name))
    super(HeapBalancerSink, self).__init__(next_provider, sink_properties, global_properties)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self._size == 0:
      channel = self._no_members
      self.__varz.no_members()
    else:
      with self._heap_lock:
        n = self.__Get()
        n.load += 1
        Heap.FixDown(self._heap, n.index, self._size)
        self._OnGet(n)

      put_called = [False]
      def PutWrapper():
        if not put_called[0]:
          with self._heap_lock:
            put_called[0] = True
            self.__Put(n)
      sink_stack.Push(self, PutWrapper)

      msg.properties[MessageProperties.Endpoint] = n.endpoint
      channel = n.channel

    channel.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    context()
    sink_stack.AsyncProcessResponse(stream, msg)

  # Events override by subclasses
  def _OnNodeDown(self, node):
    return AsyncResult.Complete()

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
          n.load -= self.Penalty
          ep = n.endpoint
          Heap.FixUp(self._heap, n.index)
          o = n.downq
          n.downq = None
          n = o
          if m is None:
            self._downq = n
          else:
            m.downq = n
          self._log.info('Marking node %s up' % str(ep))
        else:
          # No change, move to the next node in the linked list
          m, n = n, n.downq

      n = self._heap[1]
      if n.channel.state == ChannelState.Open or n.load >= 0:
        return n
      else:
        # Node is now down
        n.downq = self._downq
        self._downq = n
        n.load += self.Penalty
        Heap.FixDown(self._heap, 1, self._size)
        self._OnNodeDown(n)
        self._log.warning('Marking node %s down' % str(n.endpoint))
        # Loop

  def __Put(self, n):
    """'Return' a member to the heap.  Load on the node is decremented and its
    position in the heap is adjusted.

    Args:
      n - The node to return
    """
    n.load -= 1
    if n.load < self.Idle:
      self._log.warning('Decrementing load below Zero')
      n.load = self.Idle
    if n.index < 0 and n.load > self.Idle:
      pass
    elif n.index < 0 and n.load == self.Idle:
      n.channel.Close()
    elif n.load == self.Idle and self._size > 1:
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

  @synchronized
  def _AddSink(self, endpoint, sink_factory):
    """Add a sink to the heap.
    The sink is immediately opened and initialized to Zero load.

    Args:
      sink - The sink that was just added.
    """
    self._size += 1
    self.__varz.size(self._size)
    new_node = self.Node(sink_factory(), self.Idle, self._size, endpoint)
    self._heap.append(new_node)
    Heap.FixUp(self._heap, self._size)
    # Adding an Open() in here allows us to optimistically assume it'll be opened
    # before the next message attempts to get it.  However, the Open() will likely
    # yield, so other code paths need to be aware there is a potentially un-open
    # sink on the heap.
    if self._open:
      return self._OpenNode(new_node)
    else:
      return AsyncResult.Complete()

  def _FindNodeByEndpoint(self, endpoint):
    i = next((idx for idx, node in enumerate(self._heap)
              if node.endpoint == endpoint), 0)
    if i == 0:
      return None
    return self._heap[i]

  @synchronized
  def _RemoveSink(self, endpoint):
    """Remove a sink from the heap.
    The sink is closed immediately if it has no outstanding load, otherwise the
    close is deferred until the sink goes idle.

    Args:
      sink - The sink to be removed.
    """
    node = self._FindNodeByEndpoint(endpoint)
    if not node or node.index < 0:
      return False

    i = node.index
    Heap.Swap(self._heap, i, self._size)
    Heap.FixDown(self._heap, i, self._size - 1)
    self._heap.pop()
    self._size -= 1
    self.__varz.size(self._size)
    if self._size < 0:
      self._log.warning("Decrementing size below 0")
      self._size = 0
    node.index = -1
    if node.load == self.Idle or node.load >= 0:
      node.channel.Close()
    return True

  def _OnServersChanged(self, endpoint, channel_factory, added):
    """Invoked by the LoadBalancer when an endpoint joins or leaves the
    server set.

    Args:
      endpoint - A tuple of (endpoint, sink).
      added - True if the endpoint is being added, False if being removed.
    """
    if added:
      self._AddSink(endpoint, channel_factory)
    else:
      self._RemoveSink(endpoint)

  def _OnOpenComplete(self, ar, node):
    if ar.exception:
      self._log.error('Exception caught opening channel: %s' % str(ar.exception))
      return self._OnNodeDown(node)
    else:
      return ar

  def _OpenNode(self, n):
    return n.channel.Open().ContinueWith(lambda ar: self._OnOpenComplete(ar, n)).Unwrap()

  def Open(self):
    """Open the sink and all underlying nodes."""
    self._open = True
    if self._size > 0:
      # Ignore the first sink, it's the FailingChannelSink.
      return AsyncResult.WhenAny([self._OpenNode(n) for n in self._heap[1:]])
    else:
      return AsyncResult.Complete()

  def Close(self):
    """Close the sink and all underlying nodes immediately."""
    self._open = False
    [n.channel.Close() for n in self._heap]

  @property
  def state(self):
    return max([n.channel.state for n in self._heap])
