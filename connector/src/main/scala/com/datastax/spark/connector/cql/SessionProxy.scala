package com.datastax.spark.connector.cql

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}

import com.datastax.dse.driver.api.core.DseSession
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.util.Logging
import org.apache.commons.lang3.ClassUtils

/** Wraps a `Session` and intercepts:
  *  - `close` method to invoke `afterClose` handler
  *  - `prepare` methods to cache `PreparedStatement` objects. */
class SessionProxy(session: CqlSession, afterClose: CqlSession => Any) extends InvocationHandler {

  private var closed = false

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      (method.getName, method.getParameterTypes) match {
        case ("close", Array()) =>
          null
        case ("closeUnderlying", Array()) =>
          session.close()
          null
        case ("isClosed", Array()) =>
          closed.asInstanceOf[AnyRef]
        case _ =>
          try {
            method.invoke(session, args: _*)
          }
          catch {
            case e: InvocationTargetException =>
              throw e.getCause
          }
      }
    }
    finally {
      if (method.getName == "close" && !closed) {
        closed = true
        afterClose(session)
      }
    }
  }
}

object SessionProxy extends Logging {

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: CqlSession)(afterClose: CqlSession => Any): CqlSession = {
    val listInterfaces = ClassUtils.getAllInterfaces(session.getClass)
    val availableInterfaces = listInterfaces.toArray[Class[_]](new Array[Class[_]](listInterfaces.size))
      // DseSession has static `builder` method with incompatible return type to CqlSession.builder return type,
      // this causes errors when generating proxy. Let's exclude DseSession from proxy interfaces since it has no
      // useful methods.
      .filter(_ != classOf[DseSession])
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      availableInterfaces,
      new SessionProxy(session, afterClose)).asInstanceOf[CqlSession]
  }
}