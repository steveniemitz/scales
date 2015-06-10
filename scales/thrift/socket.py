import imp

from gevent import socket as g_socket
from thrift import transport

# Load a copy of TSocket so we can monkey patch it
TSocket = imp.load_module(
    'thrift.transport.scales_TSocket',
    *imp.find_module('TSocket', transport.__path__))
TSocket.socket = g_socket

__all__ = ['TSocket']
