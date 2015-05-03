from cStringIO import StringIO
from thrift.transport.TTransport import TFramedTransport

class TMuxTransport(TFramedTransport):
  def __init__(self, dispatcher):
    self._dispatcher = dispatcher
    self._read_future = None
    TFramedTransport.__init__(self, None)

  def flush(self):
    payload = getattr(self, '_TFramedTransport__wbuf').getvalue()
    setattr(self, '_TFramedTransport__wbuf', StringIO())
    self._read_future = self._dispatcher.SendDispatchMessage(payload, 5)

  def readFrame(self):
    if self._read_future is None:
      raise Exception("Unexpected read!")
    else:
      setattr(self, '_TFramedTransport__rbuf', self._read_future.get())
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
