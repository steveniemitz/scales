from thrift.transport.TTransport import TTransportBase

from ..compat import BytesIO
from ..http.sink import HttpTransportSinkBase
from ..sink import SinkProvider


class _ResponseReader(TTransportBase):
  CHUNK_SIZE = 4096

  def __init__(self, response, varz):
    self._stream = response.raw
    self._varz = varz
    self._rbuf = BytesIO()

  def _read_stream(self, sz):
    return self._stream.read(sz, decode_content=True)

  def read(self, sz):
    ret = self._rbuf.read(sz)
    if len(ret) != 0:
      return ret

    data = b''
    while not self._stream.closed:
      data = self._read_stream(max(sz, self.CHUNK_SIZE))
      if data:
        break

    self._varz.bytes_recv(len(data))
    self._rbuf = BytesIO(data)
    return self._rbuf.read(sz)

  def getvalue(self):
    return self._stream.read(decode_content=True)
  # TODO: Implement CReadableTransport


class ThriftHttpTransportSink(HttpTransportSinkBase):
  def __init__(self, next_provider, sink_properties, global_properties):
    self._url_suffix = sink_properties.url
    if self._url_suffix.startswith('/'):
      self._url_suffix = self._url_suffix[1:]
    super(ThriftHttpTransportSink, self).__init__(next_provider, sink_properties, global_properties)

  def _MakeRequest(self, msg, stream, timeout):
    url = 'http://%s:%s/%s' % (
        self._endpoint.host, self._endpoint.port, self._url_suffix)

    content_length = stream.tell()
    headers = {
      'Content-Type': 'application/x-thrift',
      'Content-Length': content_length
    }
    self._varz.bytes_sent(content_length)
    response = self._session.post(
        url,
        data=stream.getvalue(),
        timeout=timeout,
        headers=headers,
        stream=True)
    return response

  def _ProcessResponse(self, response, sink_stack):
    stream = _ResponseReader(response, self._varz)
    sink_stack.AsyncProcessResponseStream(stream)


ThriftHttpTransportSink.Builder = SinkProvider(
  ThriftHttpTransportSink, raise_on_http_error=True, url=None)

