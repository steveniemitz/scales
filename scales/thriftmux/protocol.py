from scales.constants import Enum

class MessageType(Enum):
  Tdispatch = 2
  Rdispatch = -2
  Rerr = -128
  BAD_Rerr = 127

  Tping = 65
  Rping = -65

  Tdiscarded = 66
  BAD_Tdiscarded = -62


class Headers(object):
  MessageType = '__MessageType'


class MuxMessage(object):
  """Base class for all Messages (transmit or response.)"""

  def __init__(self, msg_type):
    """
    Args:
      msg_type - A value from MessageType.
    """
    super(MuxMessage, self).__init__()
    self._props = {}
    self._type = msg_type

  @property
  def properties(self):
    """A collection of properties dynamically added  to the message
    during processing.
    """
    return self._props

  @property
  def type(self):
    """The type of the message."""
    return self._type

  @property
  def isResponse(self):
    """True if this message is a response type."""
    return self._type < 0

  @property
  def is_one_way(self):
    """If true, no response is expected to this message."""
    return False


class RdispatchMessage(MuxMessage):
  """A message representing a reply to a Tdispatch (method call response.)"""
  class Rstatus(Enum):
    OK = 0
    ERROR = 1
    NACK = 2

  def __init__(self, response=None, err=None):
    """
    Args:
      response - The response object that was returned by the server, if any.
      err - The error message returned by the server, if any.
    """
    super(RdispatchMessage, self).__init__(MessageType.Rdispatch)
    self._response = response
    self._err = err

  @property
  def response(self):
    return self._response

  @property
  def error_message(self):
    return self._err

class RerrorMessage(MuxMessage):
  """A message representing a server side failure of a previous Tdispatch
  message."""

  def __init__(self, err):
    super(RerrorMessage, self).__init__(MessageType.Rerr)
    self._err = err

  @property
  def error_message(self):
    return self._err

