package com.datastax.driver.spark.connector

import java.lang.reflect.{Proxy, Method, InvocationHandler}
import com.datastax.driver.core.Session

class SessionProxy(session: Session, afterClose: Session => Any) extends InvocationHandler {

  private var closed = false

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      method.invoke(session, args: _*)
    }
    finally {
      if (method.getName == "close" && !closed) {
        closed = true
        afterClose(session)
      }
    }
  }
}

object SessionProxy {  
  def withCloseAction(session: Session)(afterClose: Session => Any) =
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      Array(classOf[Session]),
      new SessionProxy(session, afterClose)).asInstanceOf[Session]
}