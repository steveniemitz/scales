class Enum(object): pass

# States <= Busy are considered open and healthy,
# > Busy are closed and unhealthy.
class ChannelState(object):
  Idle = 1
  Open = 2
  Busy = 3
  Closed = 4


class Int(object):
  MaxValue = 2147483647
  MinValue = -2147483648


class SinkProperties(object):
  Endpoint = 'endpoint'
  ServiceInterface = 'service_iface'
  Label = 'label'

class MessageProperties(object):
  Endpoint = '__Endpoint'

class SinkRole(object):
  Transport = 'transport'
  Pool = 'pool'
  LoadBalancer = 'loadbalancer'
  Formatter = 'formatter'
