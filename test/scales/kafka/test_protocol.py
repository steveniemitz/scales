import codecs
import unittest

from scales.compat import BytesIO
from scales.constants import MessageProperties
from scales.message import MethodCallMessage
from scales.kafka.protocol import (
  BrokerMetadata,
  KafkaProtocol,
  MessageType,
  MetadataResponse,
  PartitionMetadata,
  ProduceResponse,
)
from scales.kafka.sink import KafkaEndpoint

class KafkaProtocolTestCase(unittest.TestCase):
  def testPutSerialization(self):
    expected = codecs.decode(b'AAEAAAPoAAAAAQAKdGVzdF90b3BpYwAAAAEAAAABAAAAJgAAAAAAAAAAAAAAGr0KwrwAAP////8AAAAMbWVzc2FnZV9kYXRh', 'base64')
    s = KafkaProtocol()
    mcm = MethodCallMessage(None, 'Put', (b'test_topic', [b'message_data']), {})
    mcm.properties[MessageProperties.Endpoint] = KafkaEndpoint('host', 0, 1)
    buf = BytesIO()
    s.SerializeMessage(mcm, buf, {})
    self.assertEqual(buf.getvalue(), expected)

  def testPutResponseDeserialization(self):
    expected = codecs.decode(b'AAAAAgAAAAEABmxvZ2hvZwAAAAEAAAAAAAAAAAAAAA5Xsw==', 'base64')
    s = KafkaProtocol()
    ret = s.DeserializeMessage(BytesIO(expected), MessageType.ProduceRequest)
    expected = [
      ProduceResponse(b'loghog', 0, 0, 939955)
    ]
    self.assertEqual(ret.return_value, expected)

  def testBrokerInfoDeserialization(self):
    raw_data = codecs.decode(b'AAAAAgAAAAIAAAABAChlYzItNTQtODEtMTA2LTg4LmNvbXB1dGUtMS5hbWF6b25hd3MuY29tAAA+QwAAAAAAKmVjMi01NC0xNTktMTEwLTE5Mi5jb21wdXRlLTEuYW1hem9uYXdzLmNvbQAAOtcAAAABAAAABmxvZ2hvZwAAAAEACQAAAAAAAAABAAAAAgAAAAEAAAAAAAAAAgAAAAAAAAAB', 'base64')
    s = KafkaProtocol()
    ret = s.DeserializeMessage(BytesIO(raw_data), MessageType.MetadataRequest)
    expected = MetadataResponse(
      brokers = {
        0: BrokerMetadata(0, b'ec2-54-159-110-192.compute-1.amazonaws.com', 15063),
        1: BrokerMetadata(1, b'ec2-54-81-106-88.compute-1.amazonaws.com', 15939)
      },
      topics = {
        b'loghog': {
          0: PartitionMetadata(b'loghog', 0, 1, (1, 0), (0, 1))
        }
      }
    )
    self.assertEqual(ret.return_value, expected)

if __name__ == '__main__':
  unittest.main()
