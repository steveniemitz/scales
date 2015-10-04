import time
from abc import abstractmethod

import gevent
import requests
from requests import exceptions

from ..async import AsyncResult
from ..constants import ChannelState, SinkProperties
from ..sink import (ClientMessageSink, SinkProvider)
from ..message import (Deadline, MethodReturnMessage, TimeoutError)
from ..varz import VarzBase, Rate, Source

class HttpTransportSinkBase(ClientMessageSink):
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.http.HttpTransportSink'
    _VARZ = {
      'bytes_recv': Rate,
      'bytes_sent': Rate,
    }

  def __init__(self, next_provider, sink_properties, global_properties):
    super(HttpTransportSinkBase, self).__init__()
    self._endpoint = global_properties[SinkProperties.Endpoint]
    name = global_properties[SinkProperties.Label]
    self._varz = self.Varz(Source(service=name, endpoint='%s:%d' % (self._endpoint.host, self._endpoint.port)))
    self._open_result = AsyncResult.Complete()
    self._session = requests.Session()
    self._raise_on_http_error = sink_properties.raise_on_http_error

  def Open(self):
    return self._open_result

  def Close(self):
    self._session.close()
    if requests:
      self._session = requests.Session()

  @property
  def state(self):
    return ChannelState.Open

  @abstractmethod
  def _MakeRequest(self, msg, stream, timeout):
    pass

  @abstractmethod
  def _ProcessResponse(self, response, sink_stack):
    pass

  def _DoHttpRequestAsync(self, sink_stack, deadline, stream, msg):
    if deadline:
      timeout = deadline - time.time()
      if timeout < 0:
        sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=TimeoutError()))
    else:
      timeout = None

    try:
      response = self._MakeRequest(msg, stream, timeout)
      if self._raise_on_http_error and 400 <= response.status_code < 600:
        err_text = 'HTTP Error %d: %s.' % (response.status_code, response.reason)
        if response.text:
          err_text += '\nThe server returned:\n%s' % response.text
        err = exceptions.HTTPError(err_text, response=response)
        msg = MethodReturnMessage(error=err)
      else:
        self._ProcessResponse(response, sink_stack)
        return
    except exceptions.Timeout:
      msg = MethodReturnMessage(error=TimeoutError())
    except Exception as ex:
      msg = MethodReturnMessage(error=ex)
    sink_stack.AsyncProcessResponseMessage(msg)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    deadline = msg.properties.get(Deadline.KEY)
    gevent.spawn(
        self._DoHttpRequestAsync,
        sink_stack,
        deadline,
        stream,
        msg)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass


class HttpTransportSink(HttpTransportSinkBase):
  def _MakeRequest(self, msg, stream, timeout):
    url, = msg.args
    method = msg.method.lower()
    kwargs = msg.kwargs.copy()

    data = kwargs.get('data')
    if data and isinstance(data, basestring):
      self._varz.bytes_sent(len(data))

    if url.startswith('/'):
      url = url[1:]
    url = 'http://%s:%s/%s' % (self._endpoint.host, self._endpoint.port, url)
    kwargs['timeout'] = timeout
    if method == 'get':
      response = self._session.get(url, **kwargs)
    elif method == 'post':
      response = self._session.post(url, **kwargs)
    elif method == 'put':
      response = self._session.put(url, **kwargs)
    else:
      raise NotImplementedError()
    return response

  def _ProcessResponse(self, response, sink_stack):
    self._varz.bytes_recv(len(response.content))
    msg = MethodReturnMessage(response)
    sink_stack.AsyncProcessResponseMessage(msg)

HttpTransportSink.Builder = SinkProvider(
    HttpTransportSink, raise_on_http_error=True)
