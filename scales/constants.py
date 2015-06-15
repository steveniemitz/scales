class Enum(object): pass

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
  ServiceClass = 'service_cls'
  Service = 'service'
  ServerSetProvider = 'server_set_provider'
  Timeout = 'timeout'

class MessageProperties(object):
  Endpoint = '__Endpoint'
