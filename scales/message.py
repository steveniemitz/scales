"""Base classes for Message processing."""

import traceback

from scales.constants import (Enum, MessageType)

class Timeout(object):
  KEY = "__Timeout"

class Deadline(object):
  def __init__(self, timeout):
    """
    Args:
      timeout - The timeout in seconds
    """
    import  time
    self._ts = long(time.time()) * 1000000000 # Nanoseconds
    self._timeout = long(timeout * 1000000000)


class Message(object):
  """Base class for all Messages (transmit or response.)"""

  def __init__(self, msg_type):
    """
    Args:
      msg_type - A value from MessageType.
    """
    super(Message, self).__init__()
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


class TdispatchMessage(Message):
  """A message representing a method call (server dispatch.)"""

  def __init__(self, service, method, args, kwargs, ctx=None, dst=None, dtab=None):
    """
    Args:
      service - The service interface initiating the call.
      method  - The method being called.
      args    - The args passed to the method.
      kwargs  - The kwargs passed to the method.

    kwargs:
      ctx  - Optional dict of context information to be used by the sinks
             processing the message.
      dst  - Unused
      dtab - Unused
    """
    super(TdispatchMessage, self).__init__(MessageType.Tdispatch)
    self._ctx = ctx or {}
    self._dst = dst
    self._dtab = dtab
    self._service = service
    self._method = method
    self._args = args
    self._kwargs = kwargs

  @property
  def context(self):
    return self._ctx


class TpingMessage(Message):
  """A message representing a ping request."""

  def __init__(self):
    super(TpingMessage, self).__init__(MessageType.Tping)


class TdiscardedMessage(Message):
  """A message representing a request to abort processing a message on the
  server."""

  def __init__(self, which, reason):
    """
    Args:
      which - The tag being discarded.
      reason - The reason for the message being discarded.
    """
    super(TdiscardedMessage, self).__init__(MessageType.BAD_Tdiscarded)
    self._reason = reason
    self._which = which

  @property
  def is_one_way(self):
    """Tdiscarded messages are one way and expect no response."""
    return True


class RpingMessage(Message):
  """A message representing a reply to a ping request."""

  def __init__(self):
    super(RpingMessage, self).__init__(MessageType.Rping)


class RdispatchMessage(Message):
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

class RerrorMessage(Message):
  """A message representing a server side failure of a previous Tdispatch
  message."""

  def __init__(self, err):
    super(RerrorMessage, self).__init__(MessageType.Rerr)
    self._err = err

  @property
  def error_message(self):
    return self._err


class SystemMessage(object):
  """A message type reserved for internal use only.
  SystemMessages are never sent over the wire."""
  pass

class TimeoutMessage(SystemMessage):
  """A message representing a client side timeout."""
  pass

class OneWaySendCompleteMessage(SystemMessage):
  """A message returned after a one way message has been successfully sent."""
  pass

class ScalesErrorMessage(SystemMessage):
  """A message representing an internal error while processing a message."""
  def __init__(self, excr):
    self._exception = excr
    self._stack = traceback.format_exc()

  @property
  def error(self):
    return self._exception

  @property
  def stack(self):
    return self._stack

