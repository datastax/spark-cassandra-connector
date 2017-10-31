package com.datastax.spark.connector.cql

import com.datastax.driver.core.exceptions._
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement, WriteType}

/** Always retries with the same CL (null forces the original statement CL see SPARKC-494),
  *  constant number of times, regardless of circumstances
  *
  *  Retries indefinitely if maxRetryCount is -1
  */
class MultipleRetryPolicy(maxRetryCount: Int)
  extends RetryPolicy{

  // scalastyle:off null
  private def retryManyTimesOrThrow(nbRetry: Int): RetryDecision = maxRetryCount match {
    case -1 => RetryDecision.retry(null)
    case maxRetries =>
      if (nbRetry < maxRetries) {
        RetryDecision.retry(null)
      } else {
        RetryDecision.rethrow()
      }
  }

  private def retryOnceOrThrow(nbRetry: Int): RetryDecision = {
    if (nbRetry == 0) {
      RetryDecision.retry(null)
    } else {
      RetryDecision.rethrow()
    }
  }

  override def init(cluster: com.datastax.driver.core.Cluster): Unit = {}
  override def close(): Unit = { }


  override def onReadTimeout(
    stmt: Statement,
    cl: ConsistencyLevel,
    requiredResponses: Int,
    receivedResponses: Int,
    dataRetrieved: Boolean,
    nbRetry: Int): RetryDecision = {
      retryManyTimesOrThrow(nbRetry)
  }

  /**
    * We need to handle these exceptions at a level where we
    * can wait to retry.
    */
  override def onRequestError(
    stmt: Statement,
    cl: ConsistencyLevel,
    ex: DriverException,
    nbRetry: Int): RetryDecision = {
      RetryDecision.rethrow()
  }

  override def onWriteTimeout(
    stmt: Statement,
    cl: ConsistencyLevel,
    writeType: WriteType,
    requiredAcks: Int,
    receivedAcks: Int,
    nbRetry: Int): RetryDecision = {
      retryManyTimesOrThrow(nbRetry)
  }

  override def onUnavailable(
    stmt: Statement,
    cl: ConsistencyLevel,
    requiredReplica: Int,
    aliveReplica: Int,
    nbRetry: Int): RetryDecision = {
      retryManyTimesOrThrow(nbRetry)
  }

}
