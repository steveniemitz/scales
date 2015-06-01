import time
import heapq
import gevent
from gevent.event import Event

WAIT_CANCELED = -1

class TimerQueue(object):
  def __init__(self, time_source=time.time, resolution=0.05):
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
        at, seq, cancelled, action, args, kwargs = heapq.heappop(self._queue)
        if seq != peeked_seq:
          raise Exception("seq != peeked_seq")
        if not cancelled:
          # Run it
          gevent.spawn(action, *args, **kwargs)
      else:
        # A newer item came in, nothing to do here, re-loop
        pass

  def _PeekNext(self):
    return self._queue[0][:3]

  def Schedule(self, deadline, action, *args, **kwargs):
    if action is None:
      raise Exception("action must be non-null")

    if self._resolution:
      deadline = round(deadline / self._resolution) * self._resolution

    self._seq += 1
    timeout_args = [deadline, self._seq, False, action, args, kwargs]
    def cancel():
      timeout_args[2] = True
      # Null out 3-5 to avoid holding onto references.
      timeout_args[3] = None
      timeout_args[4] = None
      timeout_args[5] = None

    heapq.heappush(self._queue, timeout_args)
    # Wake up the waiter thread if this is now the newest
    if self._queue[0][0] == deadline:
      self._event.set()
    return cancel

GLOBAL_TIMER_QUEUE = TimerQueue()
