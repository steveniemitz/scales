"""Base classes for Message processing."""

import sys
import traceback

class Deadline(object):
  KEY = "__Deadline"
  EVENT_KEY = "__Deadline_Event"

  def __init__(self, timeout):
    """
    Args:
      timeout - The timeout in seconds
    """
    import  time
    self._ts = long(time.time()) * 1000000000 # Nanoseconds
    self._timeout = long(timeout * 1000000000)


class ClientError(Exception): pass
class FailedFastError(Exception): pass
class ServerError(Exception): pass
class ChannelConcurrencyError(Exception): pass
class TimeoutError(Exception):
  def __init__(self):
    super(TimeoutError, self).__init__(
      'The call did not complete within the specified timeout '
      'and has been aborted.')


class Message(object):
  """A Message represents the base class for any request."""
  __slots__ = '_properties',

  def __init__(self):
    self._properties = None

  @property
  def is_one_way(self):
    """Returns: True if this method expects no response from the server."""
    return False

  @property
  def properties(self):
    """Returns:
      A dict of properties applying to this message."""
    if not self._properties:
      self._properties = {}
    return self._properties

  @property
  def public_properties(self):
    """Returns:
      A dict of properties intended to be transported to the server
      with the method call."""
    return { k: v for k,v in self.properties.iteritems()
             if not k.startswith('__') }



class MethodCallMessage(Message):
  """A MethodCallMessage represents a method being invoked on a service."""
  __slots__ = ('service', 'method', 'args', 'kwargs')

  def __init__(self, service, method, args, kwargs):
    """
    Args:
      service - The service this method call is intended for.
      method - The method on the service.
      args - The args passed to the method call.
      kwargs-  The kwargs passed to the method call.
    """
    super(MethodCallMessage, self).__init__()
    self.service = service
    self.method = method
    self.args = args
    self.kwargs = kwargs


class MethodDiscardMessage(Message):
  """A MethodDiscardMessage represents the intent of the client to discard the
  response to a message."""
  __slots__ = ('which', 'reason')

  def __init__(self, which, reason):
    """
    Args:
      which - The MethodCallMessage that was discarded.
      reason - The reason the message is being discarded.
    """
    super(MethodDiscardMessage, self).__init__()
    self.which = which
    self.reason = reason

  @property
  def is_one_way(self):
    return True


class MethodReturnMessage(Message):
  """A message representing the return value from a service call."""
  __slots__ = ('return_value', 'error', 'stack')

  def __init__(self, return_value=None, error=None):
    """
    Args:
      return_value - The return value of the call, or None
      error - The error that occurred during processing, or None.
              If not None, the current stack will be captured and included.
    """
    super(MethodReturnMessage, self).__init__()
    self.return_value = return_value
    self.error = error
    if error:
      exc_info = sys.exc_info()
      if len(exc_info) != 3 or exc_info[2] is None:
        try:
          raise ZeroDivisionError
        except ZeroDivisionError:
          tb = sys.exc_info()[2]
          frame = tb.tb_frame.f_back
      else:
        tb = exc_info[2]
        while tb.tb_next is not None:
          tb = tb.tb_next
        frame = tb.tb_frame

      stack = traceback.format_list(traceback.extract_stack(frame))
      error_module = getattr(error, '__module__', '<builtin>')
      error_name = '%s.%s' % (error_module, error.__class__.__name__)
      stack = stack + traceback.format_exception_only(error_name, error.message)
      self.stack = stack
    else:
      self.stack = None
