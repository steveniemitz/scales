import time
import heapq
import gevent

from gevent.event import Event


class TimerQueue(object):
  """A timer that provides efficient scheduling of large numbers of events in
  the near future."""
  def __init__(self, time_source=time.time, resolution=0.05):
    """
    Args:
      time_source - A callable to get the current time (in seconds).
      resolution - The minimum resolution of the timer.  If set, all events are
                   quantized to this resolution.
    """
    self._queue = []
    self._event = Event()
    self._seq = 0
    self._resolution = resolution
    self._time_source = time_source
    gevent.spawn(self._TimerWorker)

  def _TimerWorker(self):
    while True:
      # If the queue is empty, wait for an item to be added.
      if not any(self._queue):
        self._event.wait()

      if self._event.is_set():
        self._event.clear()
        # A sleep here is needed to work around a bug with gevent.
        # If the event is cleared and then immediately waited on, the wait will
        # completed instantly and report it timed out.
        gevent.sleep(0)

      # Peek the head of the queue
      at, peeked_seq, cancelled = self._PeekNext()
      if cancelled:
        # The item has already been canceled, remove it and continue.
        heapq.heappop(self._queue)
        continue

      # Wait for a new item to be added to the queue,
      # or for the timeout to expire.
      to_wait = at - self._time_source()
      if to_wait > 0:
        # There's some time to wait, wait for it or a more recent item to be
        # added to the head of the queue.
        wait_timed_out = not self._event.wait(to_wait)
      else:
        # It should already be run, do it right now.
        wait_timed_out = True

      if wait_timed_out:
        # Nothing newer came in before it timed out.
        at, seq, cancelled, action = heapq.heappop(self._queue)
        if seq != peeked_seq:
          raise Exception("seq != peeked_seq")
        if not cancelled:
          # Run it
          gevent.spawn(action)
      else:
        # A newer item came in, nothing to do here, re-loop
        pass

  def _PeekNext(self):
    return self._queue[0][:3]

  def Schedule(self, deadline, action):
    """Schedule an operation

    Args:
      deadline - The absolute time this event should occur on.
      action - The action to run.
    Returns:
      A callable that can be invoked to cancel the scheduled operation.
    """
    if action is None:
      raise Exception("action must be non-null")

    if self._resolution:
      deadline = round(deadline / self._resolution) * self._resolution

    self._seq += 1
    timeout_args = [deadline, self._seq, False, action]
    def cancel():
      timeout_args[2] = True
      # Null out to avoid holding onto references.
      timeout_args[3] = None

    heapq.heappush(self._queue, timeout_args)
    # Wake up the waiter thread if this is now the newest
    if self._queue[0][0] == deadline:
      self._event.set()
    return cancel

GLOBAL_TIMER_QUEUE = TimerQueue()
