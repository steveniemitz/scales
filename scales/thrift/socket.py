import imp

from .. import socket_monkey
from thrift import transport

# Load a copy of TSocket so we can monkey patch it
TSocket = imp.load_module(
    'thrift.transport.scales_TSocket',
    *imp.find_module('TSocket', transport.__path__))
TSocket.socket = socket_monkey

__all__ = ['TSocket']
