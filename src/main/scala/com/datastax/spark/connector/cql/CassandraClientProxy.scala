package com.datastax.spark.connector.cql

import java.lang.reflect.{Proxy, Method, InvocationHandler}

import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.transport.TTransport

/** Extends `Cassandra.Iface` with `close` method to close the underlying thrift transport */
trait CassandraClientProxy extends Cassandra.Iface {
  def close()
}

private class ClientProxyHandler(client: Cassandra.Iface, transport: TTransport) extends InvocationHandler {
  
  override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
    if (method.getName == "close") {
      transport.close()
      null
    }
    else
      method.invoke(client, args: _*)
  }
}

object CassandraClientProxy {

  /** Returns a proxy to the thrift client that provides closing the underlying transport by calling `close` method.
    * Without this method we'd have to keep references to two objects: the client and the transport. */
  def wrap(client: Cassandra.Iface, transport: TTransport): CassandraClientProxy = {
    val classLoader = getClass.getClassLoader
    val interfaces = Array[Class[_]](classOf[CassandraClientProxy])
    val invocationHandler = new ClientProxyHandler(client, transport)
    Proxy.newProxyInstance(classLoader, interfaces, invocationHandler).asInstanceOf[CassandraClientProxy]
  }
}
