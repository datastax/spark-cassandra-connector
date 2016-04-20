package com.datastax.spark.connector.cql

import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement, WriteType}

/** Always retries with the same CL, constant number of times, regardless of circumstances */
class MultipleRetryPolicy(maxRetryCount: Int)
  extends RetryPolicy {

  private def retryManyTimesOrThrow(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < maxRetryCount) {
      RetryDecision.retry(cl)
    } else {
      RetryDecision.rethrow()
    }
  }
  
  override def init(cluster: com.datastax.driver.core.Cluster): Unit = {}
  override def close(): Unit = { }

  private def retryOnceOrThrow(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry == 0) {
      RetryDecision.retry(cl)
    } else {
      RetryDecision.rethrow()
    }
  }

  override def onReadTimeout(
      stmt: Statement,
      cl: ConsistencyLevel,
      requiredResponses: Int,
      receivedResponses: Int,
      dataRetrieved: Boolean,
      nbRetry: Int): RetryDecision = {

    retryManyTimesOrThrow(cl, nbRetry)
  }

  override def onRequestError(stmt: Statement,
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

    retryManyTimesOrThrow(cl, nbRetry)
  }

  override def onUnavailable(
      stmt: Statement,
      cl: ConsistencyLevel,
      requiredReplica: Int,
      aliveReplica: Int,
      nbRetry: Int): RetryDecision = {

    // We retry once in hope we connect to another
    // coordinator that can see more nodes (e.g. on another side of the network partition):
    retryOnceOrThrow(cl, nbRetry)
  }

}
