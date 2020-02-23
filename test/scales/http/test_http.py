import unittest

import responses

from scales.http import Http

class HttpTestCase(unittest.TestCase):

  DUMMY_HOST = 'localhost:50000'

  def setUp(self):
    self.client = Http.NewClient('tcp://%s/' % self.DUMMY_HOST)

  def _add_response(self, path, verb, json):
    url = 'http://%s/%s' % (self.DUMMY_HOST, path)
    responses.add(
      verb,
      url=url,
      json=json,
      status=200,
      content_type='application/json',
    )
    return url

  @responses.activate
  def testPost(self):
    path = 'the/path'
    body = 'param=1'
    json = {'data': 1}
    url = self._add_response(path, verb=responses.POST, json=json)

    resp = self.client.Post(path, data=body)

    self.assertEqual(resp.json(), json)

    self.assertEqual(responses.calls[0].request.url, url)
    self.assertEqual(responses.calls[0].request.body, body)
