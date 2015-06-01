package com.twitter.finagle.example.thrift

import com.twitter.finagle.example.thriftscala.Hello
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.util.{Await, Future}

object ThriftServer {
  def main(args: Array[String]) {
    //#thriftserverapi
    val server = ThriftMux.Server()
         .configured(Label("thrift"))
         .configured(TimeoutFilter.Param(100.milliseconds))
         .serve(8080, new Hello[Future] {
	      def hi() = Future.value("hi")
	    })

    //val server = Thrift.serveIface("localhost:8080", new Hello[Future] {
    // def hi() = Future.value("hi")
    //})
    Await.ready(server)
    //#thriftserverapi
  }
}