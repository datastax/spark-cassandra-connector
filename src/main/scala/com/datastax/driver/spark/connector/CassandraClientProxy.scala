package com.datastax.driver.spark.connector

import java.lang.reflect.{Proxy, Method, InvocationHandler}

import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.transport.TTransport

trait CassandraClientProxy extends Cassandra.Iface {
  def close()
}

class ClientProxyHandler(client: Cassandra.Iface, transport: TTransport) extends InvocationHandler {
  
  override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
    if (method.getName == "close") {
      transport.close()
      null
    }
    else
      method.invoke(client, args: _*)
  }
}

object ClientProxy {

  def wrap(client: Cassandra.Iface, transport: TTransport): CassandraClientProxy = {
    val classLoader = getClass.getClassLoader
    val interfaces = Array[Class[_]](classOf[CassandraClientProxy])
    val invocationHandler = new ClientProxyHandler(client, transport)
    Proxy.newProxyInstance(classLoader, interfaces, invocationHandler).asInstanceOf[CassandraClientProxy]
  }
}
