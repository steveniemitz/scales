from cStringIO import StringIO
from thrift.transport.TTransport import TFramedTransport

class TMuxTransport(TFramedTransport):
  def __init__(self, dispatcher_pool):
    self._dispatcher_pool = dispatcher_pool
    self._read_future = None
    TFramedTransport.__init__(self, None)

  def flush(self):
    payload = getattr(self, '_TFramedTransport__wbuf')
    setattr(self, '_TFramedTransport__wbuf', StringIO())
    _, dispatcher = self._dispatcher_pool.Get()
    self._read_future = dispatcher.SendDispatchMessage(payload)

  def readFrame(self):
    if self._read_future is None:
      raise Exception("Unexpected read!")
    else:
      msg = self._read_future.get()
      setattr(self, '_TFramedTransport__rbuf', msg.buf)
      self._read_future = None

  def open(self):
    # never touch the underlying transport
    pass

  def isOpen(self):
    # always open
    return True

  def close(self):
    # never touch it
    pass
