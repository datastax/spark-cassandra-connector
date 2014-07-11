package com.datastax.spark.connector.cql

import java.lang.reflect.{Proxy, Method, InvocationHandler}
import com.datastax.driver.core.{RegularStatement, SimpleStatement, Session}
import org.apache.avro.generic.GenericData.StringType

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
        case ("prepare", Array(StringClass)) =>
          PreparedStatementCache.prepareStatement(session, new SimpleStatement(args(0).asInstanceOf[String]))
        case ("prepare", Array(RegularStatementClass)) =>
          PreparedStatementCache.prepareStatement(session, args(0).asInstanceOf[RegularStatement])
        case _ => method.invoke(session, args: _*)
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

object SessionProxy {

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * The proxy adds prepared statement caching functionality. */
  def wrap(session: Session): Session =
    wrapWithCloseAction(session)(_ => ())

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: Session)(afterClose: Session => Any): Session =
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      Array(classOf[Session]),
      new SessionProxy(session, afterClose)).asInstanceOf[Session]
}