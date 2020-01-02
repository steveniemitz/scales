from collections import namedtuple
from struct import (pack, unpack)
import time

from ..compat import BytesIO
from ..dispatch import MessageDispatcher
from ..loadbalancer.serverset import StaticServerSetProvider
from ..loadbalancer.zookeeper import Member
from ..message import (
  MethodCallMessage,
  MethodReturnMessage
)
from ..mux.sink import MuxSocketTransportSink
from ..sink import (
  ClientMessageSink,
  SinkProperties,
  SinkProvider,
  SinkRole,
)
from ..thrift.sink import SocketTransportSinkProvider

from .protocol import (
  BrokerMetadata,
  ErrorCode,
  KafkaError,
  KafkaProtocol,
  MessageHelper,
  NoBrokerForTopicException,
)

KafkaEndpoint = namedtuple("KafkaEndpoint", "host port partition_id")
KafkaEndpoint.__str__ = lambda k: '%s:%d' % (k.host, k.port)

class TopicState(object):
  def __init__(self, topic_name, lb, brokers, open_ar):
    self.topic_name = topic_name
    self.lb = lb
    self.brokers = brokers
    self.last_refreshed = time.time()
    self.open_ar = open_ar


class KafkaTransportSink(MuxSocketTransportSink):
  CLIENT_ID = 'scales'

  def _CheckInitialConnection(self):
    pass

  def _OnTimeout(self, tag):
    pass

  def _BuildHeader(self, tag, msg_type, data_len):
    header = pack(
      '!ihhih%ds' % len(self.CLIENT_ID),
      2 + 2 + 4 + 2 + len(self.CLIENT_ID) + data_len,
      msg_type,
      0,
      tag,
      len(self.CLIENT_ID),
      self.CLIENT_ID
    )
    return header

  def _ProcessReply(self, stream):
    tag, = unpack('!i', stream.read(4))
    self._ProcessTaggedReply(tag, stream)


class KafkaRouterSink(ClientMessageSink):
  _RETRY_KEY = "__KafkaRetries"
  _BOOTSTRAP_SUFFIX = "-bootstrap"

  def __init__(self, next_provider, sink_properties, global_properties):
    self._metadata = None
    self._topics = {}
    self._next_provider = next_provider
    self._global_properties = global_properties
    self._label = global_properties['label']
    self._refresh_rate = sink_properties.refresh_rate
    self._refresh_ar = None
    self._bootstrap_servers = set()
    self._bootstrap_lb = self._CreateBootstrapLoadBalancer(
      sink_properties.server_set_provider)
    super(KafkaRouterSink, self).__init__()

  def _CreateBootstrapLoadBalancer(self, broker_provider):
    """Create a load balancer used to bootstrap the cluster.

    Args:
      broker_provider: A ServerSetProvider that contains at least one kafka
                       broker.
    Returns:
      A load balancer that can be used to bootstrap the kafka client.
    """
    brokers = []
    broker_provider.Initialize(None, None)
    for bs in broker_provider.GetServers():
      ep = bs.service_endpoint
      brokers.append((-1, BrokerMetadata(-1, ep.host, ep.port)))

    self._bootstrap_servers = set([(b.host, b.port) for _, b in brokers])
    return self._CreateBrokerLoadBalancer(brokers, self._BOOTSTRAP_SUFFIX)

  def _RefreshMetadataAsync(self):
    """Asynchronously refreshes the kafka cluster metadata.

    Returns:
      An AsyncResult representing the refresh operation.
    """
    if not self._refresh_ar:
      self._refresh_ar = self._bootstrap_lb.Open().ContinueWith(
      lambda ar: MessageDispatcher.StaticDispatchMessage(
        self._bootstrap_lb,
        None,
        0,
        0,
        MethodCallMessage(None, '__metadata', [], {}))
      ).Unwrap()
    return self._refresh_ar

  def _CreateBrokerLoadBalancer(self, brokers, service_suffix):
    """Create a load balancer for a set of brokers.

    Args:
      brokers - A list of (partition_id, BrokerMetadata) tuples.
      service_suffix - An optional suffix to add the to generated service.

    Returns:
      A new load balancer sink populated with the brokers passed in.
    """
    members = [
      Member(b.nodeId, KafkaEndpoint(b.host, b.port, partition_id), {}, b.nodeId, 'ALIVE')
      for partition_id, b in brokers
    ]
    ssp = StaticServerSetProvider(members)
    balancer = self._next_provider.Clone(server_set_provider=ssp)
    return balancer.CreateSink({ SinkProperties.Label: self._label + (service_suffix or '') })

  def _RebuildTopicLoadBalancers(self, broker_metadata, requested_topic_name):
    """Creates a new set of load balancers for all topics.  Each load balancer
    consists of the leaders for all partitions in a topic.

    Args:
      broker_metadata - A MetadataResponse object.
      topic_name - The topic to refresh
    """
    brokers = broker_metadata.brokers
    new_topics = {}
    old_lbs_to_close = []
    seen_new_topics = set()

    for topic_name, t in broker_metadata.topics.items():
      partition_leaders = set((p.partition_id, p.leader)
                              for p in t.values() if p.leader != -1)
      topic_brokers = set((partition_id, brokers[broker])
                       for partition_id, broker in partition_leaders)
      topic_state = self._topics.get(topic_name)
      if topic_state and topic_state.brokers == topic_brokers:
        topic_state.last_refreshed = time.time()
      elif topic_name == requested_topic_name:
        if topic_state:
          old_lbs_to_close.append(topic_state.lb)
        lb = self._CreateBrokerLoadBalancer(topic_brokers, None)
        open_ar = lb.Open()
        topic_state = TopicState(topic_name, lb, topic_brokers, open_ar)
      else:
        seen_new_topics.add(topic_name)

      if topic_state:
        new_topics[topic_name] = topic_state

    self._topics = new_topics
    for lb in old_lbs_to_close:
      lb.Close()

    new_bootstrap_keys = set((b.host, b.port) for b in brokers.values())
    if new_bootstrap_keys != self._bootstrap_servers:
      new_bootstrap = self._CreateBrokerLoadBalancer(brokers.items(), self._BOOTSTRAP_SUFFIX)
      old_bootstrap = self._bootstrap_lb
      self._bootstrap_lb = new_bootstrap
      old_bootstrap.Close()

  def _GetTopic(self, topic):
    return self._topics.get(topic)

  def _RefreshBrokersAndRetry(self, topic_name, sink_stack, msg):
    topic_info = self._GetTopic(topic_name)
    if topic_info and (
        time.time() - topic_info.last_refreshed < self._refresh_rate):
      # We've already tried to refresh within the refresh rate, just send it
      self._AsyncProcessRequestToTopic(sink_stack, msg, topic_info)
      return

    refresh_ar = self._RefreshMetadataAsync()
    def _on_refresh_complete(ar):
      self._refresh_ar = None
      if ar.exception:
        sink_stack.AsyncProcessResponseMessage(
          MethodReturnMessage(error=ar.exception))
      else:
        try:
          # Rebuild
          self._RebuildTopicLoadBalancers(ar.value, topic_name)
          # Retry
          topic, _, _ = MessageHelper.GetPutArgs(msg)
          topic_info = self._GetTopic(topic)
          if not topic_info:
            sink_stack.AsyncProcessResponseMessage(
              MethodReturnMessage(error=NoBrokerForTopicException("No broker for topic %s" % topic))
            )
          else:
            # We're in the gevent hub here, move to a new greenlet to do the retry.
            self._AsyncProcessRequestToTopic(sink_stack, msg, topic_info)
        except Exception as ex:
          sink_stack.AsyncProcessResponseMessage(MethodReturnMessage(error=ex))
    refresh_ar.ContinueWith(_on_refresh_complete, False)

  def _AsyncProcessRequestToTopic(self, sink_stack, msg, topic_info):
    def _send_msg(_):
      sink_stack.Push(self, (msg, topic_info.topic_name))
      topic_info.lb.AsyncProcessRequest(sink_stack, msg, None, {})
    if not topic_info.open_ar:
      topic_info.open_ar = topic_info.lb.Open()
    topic_info.open_ar.ContinueWith(_send_msg, False)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    topic, _, _ = MessageHelper.GetPutArgs(msg)
    topic_info = self._GetTopic(topic)
    if not topic_info:
      self._RefreshBrokersAndRetry(topic, sink_stack, msg)
    else:
      self._AsyncProcessRequestToTopic(sink_stack, msg, topic_info)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    req_msg, topic_name = context
    num_retries = req_msg.properties.get(self._RETRY_KEY, 0)
    if msg.return_value:
      if len(msg.return_value) != 1:
        sink_stack.AsyncProcessResponseMessage(
          MethodReturnMessage(error=Exception('Invalid response from kafka: unexpected return value')))
      else:
        err_code = msg.return_value[0].error
        if err_code == ErrorCode.NoError:
          sink_stack.AsyncProcessResponseMessage(msg)
        elif err_code in ErrorCode.ReloadMetadataCodes and num_retries == 0:
          # Refresh metadata and retry
          req_msg.properties[self._RETRY_KEY] = 1
          self._RefreshBrokersAndRetry(topic_name, sink_stack, req_msg)
        else:
          err_msg = MethodReturnMessage(error=KafkaError(
              'Kafka broker returned error %d' % err_code, err_code))
          sink_stack.AsyncProcessResponseMessage(err_msg)
    else:
      # An exception occured, retry once
      self._RefreshBrokersAndRetry(topic_name, sink_stack, req_msg)


class KafkaSerializerSink(ClientMessageSink):
  def __init__(self, next_provider, sink_properties, global_properties):
    super(KafkaSerializerSink, self).__init__()
    self._serializer = KafkaProtocol()
    self.next_sink = next_provider.CreateSink(global_properties)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    buf = BytesIO()
    headers = {}

    try:
      ctx = self._serializer.SerializeMessage(msg, buf, headers)
    except Exception as ex:
      msg = MethodReturnMessage(error=ex)
      sink_stack.AsyncProcessResponseMessage(msg)
      return

    sink_stack.Push(self, ctx)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, buf, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    if msg:
      sink_stack.AsyncProcessResponseMessage(msg)
    else:
      try:
        msg = self._serializer.DeserializeMessage(stream, context)
      except Exception as ex:
        msg = MethodReturnMessage(error=ex)
      sink_stack.AsyncProcessResponseMessage(msg)


KafkaRouterSink.Builder = SinkProvider(
  KafkaRouterSink,
  SinkRole.LoadBalancer,
  server_set_provider = None,
  refresh_rate = 10
)
KafkaSerializerSink.Builder = SinkProvider(KafkaSerializerSink)
KafkaTransportSink.Builder = SocketTransportSinkProvider(KafkaTransportSink)
