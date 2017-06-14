package com.datastax.spark.connector.cql

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}

import com.datastax.spark.connector.util.Logging
import com.datastax.driver.core.{RegularStatement, Session, SimpleStatement}
import org.apache.commons.lang3.ClassUtils

import collection.JavaConverters._

/** Wraps a `Session` and intercepts:
  *  - `close` method to invoke `afterClose` handler
  *  - `prepare` methods to cache `PreparedStatement` objects. */
class SessionProxy(session: Session, afterClose: Session => Any) extends InvocationHandler {

  private var closed = false

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      val StringClass = classOf[String]
      val RegularStatementClass = classOf[String]

      (method.getName, method.getParameterTypes) match {
        case ("close", Array()) =>
          null
        case ("closeUnderlying", Array()) =>
          session.close()
          null
        case ("isClosed", Array()) =>
          closed.asInstanceOf[AnyRef]
        case ("prepare", Array(StringClass)) =>
          PreparedStatementCache.prepareStatement(session, new SimpleStatement(args(0).asInstanceOf[String]))
        case ("prepare", Array(RegularStatementClass)) =>
          PreparedStatementCache.prepareStatement(session, args(0).asInstanceOf[RegularStatement])
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
    * The proxy adds prepared statement caching functionality. */
  def wrap(session: Session): Session =
    wrapWithCloseAction(session)(_ => ())

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: Session)(afterClose: Session => Any): Session = {
    val listInterfaces = ClassUtils.getAllInterfaces(session.getClass)
    val availableInterfaces = listInterfaces.toArray[Class[_]](new Array[Class[_]](listInterfaces.size))
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      availableInterfaces,
      new SessionProxy(session, afterClose)).asInstanceOf[Session]
  }
}