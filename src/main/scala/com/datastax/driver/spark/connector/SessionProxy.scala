package com.datastax.driver.spark.connector

import java.lang.reflect.{Proxy, Method, InvocationHandler}
import com.datastax.driver.core.Session

/** Wraps a `Session` and intercepts `close` method to invoke `afterClose` handler. */
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

  /** Registers a callback on `Session#close` method.
    * @param afterClose code to be invoked after the session has been closed */
  def withCloseAction(session: Session)(afterClose: Session => Any): Session =
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      Array(classOf[Session]),
      new SessionProxy(session, afterClose)).asInstanceOf[Session]
}