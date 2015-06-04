"""A module that can stand in for the builtin socket module, but uses a gevent
socket instead of a normal one"""

from socket import *
from gevent.socket import socket

