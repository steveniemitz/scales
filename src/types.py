class MessageType(object):
  Tdispatch = 2
  Rdispatch = -2

  Tping = 65
  Rping = -65

  Tdiscarded = 66
  BAD_Tdiscarded = -62


class DispatcherState(object):
  STARTING = 1
  RUNNING = 2
  SHUTTING_DOWN = 3
  STOPPED = 4
  FAULTED = 5
  PING_TIMEOUT = 6
