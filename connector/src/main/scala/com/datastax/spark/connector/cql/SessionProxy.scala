package com.datastax.spark.connector.cql

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.concurrent.{CompletableFuture, CompletionStage}

import com.datastax.dse.driver.api.core.DseSession
import com.datastax.dse.driver.internal.core.session.DefaultDseSession
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.util.Logging
import org.apache.commons.lang3.ClassUtils

private[cql] trait CloseHandler {
  var closed = false

  def afterClose: CqlSession => Any

  def session: CqlSession

  def onClose(): Any = {
    if (!closed) {
      closed = true
      afterClose(session)
    }
  }
}

/** Since java Proxy is not able to produce proxy for DseSession, this wrapper provides similar functionality
  * (intercepts `close` invocations) for this special case. Other `CqlSession` types are handled as usual. */
private[cql] class DseSessionWrapper(val session: DseSession, val afterClose: CqlSession => Any)
  extends DefaultDseSession(session)
    with CloseHandler {

  override def close(): Unit = {
    onClose()
  }

  override def isClosed: Boolean = closed

  override def closeAsync(): CompletionStage[Void] = {
    onClose()
    CompletableFuture.completedFuture(null)
  }
}

/** Wraps a `Session` and intercepts:
  *  - `close` method to invoke `afterClose` handler
  *  - `prepare` methods to cache `PreparedStatement` objects. */
class SessionProxy(val session: CqlSession, val afterClose: CqlSession => Any) extends InvocationHandler with CloseHandler {

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      (method.getName, method.getParameterTypes) match {
        case ("close", Array()) =>
          onClose()
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
      if (method.getName == "close") {
        onClose()
      }
    }
  }
}

object SessionProxy extends Logging {

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    *
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: CqlSession)(afterClose: CqlSession => Any): CqlSession = {
    session match {
      case dseSession: DseSession => new DseSessionWrapper(dseSession, afterClose)
      case other =>
        val listInterfaces = ClassUtils.getAllInterfaces(session.getClass)
        val availableInterfaces = listInterfaces.toArray[Class[_]](new Array[Class[_]](listInterfaces.size))
        Proxy.newProxyInstance(
          session.getClass.getClassLoader,
          availableInterfaces,
          new SessionProxy(other, afterClose)).asInstanceOf[CqlSession]
    }
  }
}
