import traceback
from struct import (pack)

from ttypes import (Enum, MessageType)

class Marshallable(object):
  def Marshal(self, buf):
    raise NotImplementedError()

  @classmethod
  def Unmarshal(cls, buf, ctx):
    raise NotImplementedError()


class Tmessage(object): pass
class Rmessage(object): pass

class Timeout(object):
  KEY = "__Timeout"

class Deadline(Marshallable):
  def __init__(self, timeout):
    """
    Args:
      timeout - The timeout in seconds
    """
    import  time
    self._ts = long(time.time()) * 1000000000 # Nanoseconds
    self._timeout = long(timeout * 1000000000)

  def __len__(self):
    return 16

  def Marshal(self, buf):
    buf.write(pack('!qq', self._ts, self._timeout))


class Message(object):
  def __init__(self, msg_type):
    super(Message, self).__init__()
    self._props = {}
    self._type = msg_type

  @property
  def properties(self):
    return self._props

  @property
  def type(self):
    return self._type

  @property
  def isResponse(self):
    return self._type < 0

  @property
  def is_one_way(self):
    return False

class TdispatchMessage(Message):
  def __init__(self, service, method, args, kwargs, ctx=None, dst=None, dtab=None):
    super(TdispatchMessage, self).__init__(MessageType.Tdispatch)
    self._ctx = ctx or {}
    self._dst = dst
    self._dtab = dtab
    self._service = service
    self._method = method
    self._args = args
    self._kwargs = kwargs


class TpingMessage(Message):
  def __init__(self):
    super(TpingMessage, self).__init__(MessageType.Tping)


class TdiscardedMessage(Message):
  def __init__(self, which, reason):
    super(TdiscardedMessage, self).__init__(MessageType.BAD_Tdiscarded)
    self._reason = reason
    self._which = which

  @property
  def is_one_way(self):
    return True


class RpingMessage(Message):
  def __init__(self):
    super(RpingMessage, self).__init__(MessageType.Rping)


class RdispatchMessage(Message):
  class Rstatus(Enum):
    OK = 0
    ERROR = 1
    NACK = 2

  def __init__(self, response=None, err=None):
    super(RdispatchMessage, self).__init__(MessageType.Rdispatch)
    self._response = response
    self._err = err

  @property
  def response(self):
    return self._response

  @property
  def error_message(self):
    return self._err

class RerrorMessage(Message):
  def __init__(self, err):
    super(RerrorMessage, self).__init__(MessageType.Rerr)
    self._err = err

  @property
  def error_message(self):
    return self._err


class SystemMessage(object): pass
class TimeoutMessage(SystemMessage): pass
class OneWaySendCompleteMessage(SystemMessage): pass
class SystemErrorMessage(SystemMessage):
  def __init__(self, excr):
    self._exception = excr
    self._stack = traceback.format_exc()

  @property
  def error(self):
    return self._exception

  @property
  def stack(self):
    return self._stack

