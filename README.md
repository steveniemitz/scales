# scales
A protocol agnostic RPC client stack for python.

## Features
* Built in support for thrift and thrift-mux (for finagle servers, see http://twitter.github.io/finagle/guide/Protocols.html)
* Extensible stack for easy support of other protocols.
* Fully asynchronous API
* Robust load balancing and error detection / recovery.
* Service discovery via ZooKeeper
