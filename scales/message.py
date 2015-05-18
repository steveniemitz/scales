"""Base classes for Message processing."""

import sys
import traceback

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


class ServerError(Exception): pass
class ScalesClientError(Exception): pass
class TimeoutError(Exception):
  def __init__(self):
    super(TimeoutError, self).__init__(
      'The call did not complete within the specified timeout '
      'and has been aborted.')


class Message(object):
  def __init__(self):
    self._properties = None

  @property
  def is_one_way(self):
    return False

  @property
  def properties(self):
    if not self._properties:
      self._properties = {}
    return self._properties


class MethodCallMessage(Message):
  def __init__(self, service, method, args, kwargs):
    super(MethodCallMessage, self).__init__()
    self.service = service
    self.method = method
    self.args = args
    self.kwargs = kwargs

  @property
  def public_properties(self):
    return { k: v for k,v in self.properties.iteritems()
            if not k.startswith('__') }


class MethodDiscardMessage(Message):
  def __init__(self, which, reason):
    super(MethodDiscardMessage, self).__init__()
    self.which = which
    self.reason = reason

  @property
  def is_one_way(self):
    return True


class MethodReturnMessage(Message):
  def __init__(self, return_value=None, error=None):
    super(MethodReturnMessage, self).__init__()
    self.return_value = return_value
    self.error = error
    if error:
      if sys.exc_info()[0]:
        self.stack = traceback.format_exc()
      else:
        try:
          raise ZeroDivisionError
        except ZeroDivisionError:
          tb = sys.exc_info()[2]
        stack = traceback.format_list(traceback.extract_stack(tb.tb_frame.f_back))
        error_name = '%s.%s' % (error.__module__, error.__class__.__name__)
        stack = stack + traceback.format_exception_only(error_name, error.message)
        self.stack = stack
    else:
      self.stack = None
