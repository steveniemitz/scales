class MessageType(object):
  Tdispatch = 2
  Rdispatch = -2

  Tping = 65
  Rping = -65

  Tdiscarded = 66
  BAD_Tdiscarded = -62


class DispatcherState(object):
  RUNNING = 1
  FAULTED = 2
