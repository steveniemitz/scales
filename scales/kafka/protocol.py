from cStringIO import StringIO

from struct import pack, Struct
from collections import namedtuple
from ..binary import (
  BinaryReader,
  BinaryWriter
)
from ..constants import TransportHeaders
from ..message import MethodReturnMessage
from ..constants import MessageProperties

import zlib

class MessageHelper(object):
  @staticmethod
  def GetPutArgs(msg):
    def _get_put_args(topic, payloads=[], acks=1):
      return topic, payloads, acks
    return _get_put_args(*msg.args, **msg.kwargs)

class MessageType(object):
  MetadataRequest = 3
  ProduceRequest = 0

class ErrorCode(object):
  NoError = 0
  Unknown = -1
  OffsetOutOfRange = 1
  InvalidMessage = 2
  UnknownTopicOrPartition = 3
  InvalidMessageSize = 4
  LeaderNotAvailable = 5
  NotLeaderForPartition = 6
  RequestTimedOut = 7
  BrokerNotAvailable = 8
  ReplicaNotAvailable = 9
  MessageSizeTooLarge = 10
  StaleControllerEpochCode = 11
  OffsetMetadataTooLargeCode = 12
  OffsetsLoadInProgressCode = 14
  ConsumerCoordinatorNotAvailableCode = 15
  NotCoordinatorForConsumerCode = 16

  ReloadMetadataCodes = (
    UnknownTopicOrPartition,
    NotLeaderForPartition,
    BrokerNotAvailable
  )

  _ErrorCodeLookup = {
   -1: 'Unknown',
    0: 'NoError',
    1: 'OffsetOutOfRange',
    2: 'InvalidMessage',
    3: 'UnknownTopicOrPartition',
    4: 'InvalidMessageSize',
    5: 'LeaderNotAvailable',
    6: 'NotLeaderForPartition',
    7: 'RequestTimedOut',
    8: 'BrokerNotAvailable',
    9: 'ReplicaNotAvailable',
    10: 'MessageSizeTooLarge',
    11: 'StaleControllerEpochCode',
    12: 'OffsetMetadataTooLargeCode',
    14: 'OffsetsLoadInProgressCode',
    15: 'ConsumerCoordinatorNotAvailableCode',
    16: 'NotCoordinatorForConsumerCode',
  }
  @staticmethod
  def MessageForCode(code):
    return ErrorCode._ErrorCodeLookup[code]


class KafkaError(Exception):
  def __init__(self, message, error_code):
    self.message = message
    self.error_code = error_code
    super(KafkaError, self).__init__(message)

class NoBrokerForTopicException(Exception): pass

MetadataResponse = namedtuple('MetadataResponse', 'brokers topics')
BrokerMetadata = namedtuple('BrokerMetadata', 'nodeId host port')
PartitionMetadata = namedtuple('PartitionMetadata', 'topic_name partition_id leader replicas isr')
ProduceResponse = namedtuple('ProduceResponse', 'topic partition error offset')

class KafkaProtocol(object):
  MSG_STRUCT = Struct('!BBii')
  MSG_HEADER = Struct('!qii')
  PRODUCE_HEADER = Struct('!hii')

  def DeserializeMessage(self, buf, msg_type):
    # Skip the correlationId
    buf.read(4)

    if msg_type == MessageType.MetadataRequest:
      return self._DeserializeMetadataResponse(buf)
    elif msg_type == MessageType.ProduceRequest:
      return self._DeserializeProduceResponse(buf)

  def SerializeMessage(self, msg, buf, headers):
    if msg.method == '__metadata':
      self._SerializeMetadataRequest(msg, buf, headers)
      return MessageType.MetadataRequest
    if msg.method == 'Put':
      self._SerializeProduceRequest(msg, buf, headers)
      return MessageType.ProduceRequest
    else:
      raise NotImplementedError()

  def _SerializeMetadataRequest(self, msg, buf, headers):
    headers[TransportHeaders.MessageType] = MessageType.MetadataRequest
    buf.write(pack('!i', len(msg.args or [])))
    for topic in msg.args:
      buf.write(pack('!h'), len(topic))
      buf.write(topic)

  def _DeserializeMetadataResponse(self, buf):
    reader = BinaryReader(buf)

    num_brokers = reader.ReadInt32()
    brokers = {}
    for n in range(num_brokers):
      nodeId = reader.ReadInt32()
      host = reader.ReadString()
      port = reader.ReadInt32()
      brokers[nodeId] = BrokerMetadata(nodeId, host, port)

    num_topics = reader.ReadInt32()
    topic_metadata = {}
    for t in range(num_topics):
      topic_error_code = reader.ReadInt16() #Unused
      topic_name = reader.ReadString()
      num_partitions = reader.ReadInt32()

      partition_data = {}
      for p in range(num_partitions):
        error_code, partition_id, leader = reader.Unpack('!hii')
        replicas = reader.ReadInt32Array()
        isr = reader.ReadInt32Array()
        partition_data[partition_id] = PartitionMetadata(
          topic_name, partition_id, leader, replicas, isr)

      topic_metadata[topic_name] = partition_data
    return MethodReturnMessage(MetadataResponse(brokers, topic_metadata))

  def _GetMessageHeader(self, payload):
    return self.MSG_STRUCT.pack(0, 0, -1, len(payload))

  def _SerializeProduceRequest(self, msg, buf, headers):
    headers[TransportHeaders.MessageType] = MessageType.ProduceRequest
    topic, payloads, acks = MessageHelper.GetPutArgs(msg)
    endpoint = msg.properties[MessageProperties.Endpoint]

    writer = BinaryWriter(buf)

    # Header, 1000ms timeout, 1 message set
    writer.WriteStruct(self.PRODUCE_HEADER, acks, 1000, 1)

    writer.WriteString(topic)
    writer.WriteInt32(1) # 1 payload
    writer.WriteInt32(endpoint.partition_id)

    # [Offset (8) + HeaderLen (4) + CRC (4) + payload (P) + header(10)] x N messages
    msg_set_len = sum([8 + 4 + 4 + len(p) + 10 for p in payloads])
    writer.WriteInt32(msg_set_len)

    for p in payloads:
      header = self._GetMessageHeader(p)
      # Calculate the CRC
      crc = zlib.crc32(header)
      crc = zlib.crc32(p, crc)

      # Write the header
      writer.WriteStruct(self.MSG_HEADER, 0, len(header) + len(p) + 4, crc)
      # Write the message data
      writer.WriteRaw(header)
      writer.WriteRaw(p)

  def _DeserializeProduceResponse(self, buf):
    reader = BinaryReader(buf)
    num_topics = reader.ReadInt32()
    responses = []
    for t in range(num_topics):
      topic = reader.ReadString()
      num_partitions = reader.ReadInt32()
      for p in range(num_partitions):
        partition = reader.ReadInt32()
        error_code = reader.ReadInt16()
        offset = reader.ReadInt64()
        responses.append(ProduceResponse(topic, partition, error_code, offset))
    return MethodReturnMessage(responses)
