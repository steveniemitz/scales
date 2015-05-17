import collections

class VarzReceiver(object):
  VARZ_DATA = collections.defaultdict(int)

  @staticmethod
  def IncrementVarz(metric, source, amount):
    VarzReceiver.VARZ_DATA[(metric, source)] += amount

  @staticmethod
  def SetVarz(metric, source, value):
    VarzReceiver.VARZ_DATA[(metric, source)] = value

  @staticmethod
  def GetAllVarz():
    return VarzReceiver.VARZ_DATA