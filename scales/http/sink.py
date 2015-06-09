import time

import gevent
import requests
from requests import exceptions

from ..async import AsyncResult
from ..constants import ChannelState, SinkProperties
from ..sink import (ClientMessageSink, SinkProvider)
from ..message import (MethodReturnMessage, TimeoutError)

class HttpTransportSink(ClientMessageSink):
  def __init__(self, next_provider, properties):
    super(HttpTransportSink, self).__init__()
    self._endpoint = properties[SinkProperties.Endpoint]
    self._open_result = AsyncResult.Complete()
    self._session = requests.Session()

  def Open(self):
    return self._open_result

  def Close(self):
    self._session.close()
    self._session = requests.Session()

  @property
  def state(self):
    return ChannelState.Open

  def _DoHttpRequestAsync(self, sink_stack, deadline, method, url, **kwargs):
    if url.startswith('/'):
      url = url[1:]
    method = method.lower()

    if deadline:
      timeout = deadline - time.time()
      if timeout < 0:
        sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=TimeoutError()))
    else:
      timeout = None

    url = 'http://%s:%s/%s' % (self._endpoint.host, self._endpoint.port, url)
    try:
      if method == 'get':
        response = self._session.get(url, timeout=timeout, data=kwargs)
      elif method == 'post':
        response = self._session.post(url, timeout=timeout, data=kwargs)
      else:
        raise NotImplementedError()

      if 400 <= response.status_code < 600:
        err_text = 'HTTP Error %d: %s.' % (response.status_code, response.reason)
        if response.text:
          err_text += '\nThe server returned:\n%s' % response.text
        err = exceptions.HTTPError(err_text, response=response)
        msg = MethodReturnMessage(error=err)
      else:
        msg = MethodReturnMessage(response)
    except exceptions.Timeout:
      msg = MethodReturnMessage(error=TimeoutError())
    except Exception as ex:
      msg = MethodReturnMessage(error=ex)

    sink_stack.AsyncProcessResponseMessage(msg)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    deadline = msg.properties.get(SinkProperties.Timeout)
    gevent.spawn(
        self._DoHttpRequestAsync,
        sink_stack,
        deadline,
        msg.method,
        *msg.args,
        **msg.kwargs)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass

HttpTransportSinkProvider = SinkProvider(HttpTransportSink)
