

class Sink(object):
  def __init__(self, next_sink):
    self._next = next_sink

  @property
  def next_sink(self):
    return self._next

  def ProcessMessage(self, msg, stream):
    raise NotImplementedError()

  def ProcessReply(self, msg, stream):
    raise NotImplementedError()


class ThriftSocketTransportSink(Sink):
  def ProcessMessage(self, msg, stream):
    pass
