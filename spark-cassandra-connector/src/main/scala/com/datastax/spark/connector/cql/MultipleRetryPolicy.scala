package com.datastax.spark.connector.cql

import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement, WriteType}

/** Always retries with the same CL, constant number of times, regardless of circumstances */
class MultipleRetryPolicy(maxRetryCount: Int, retryDelay: CassandraConnectorConf.RetryDelayConf)
  extends RetryPolicy {

  private def retryOrThrow(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < maxRetryCount) {
      if (nbRetry > 0) {
        val delay = retryDelay.forRetry(nbRetry).toMillis
        if (delay > 0) Thread.sleep(delay)
      }
      RetryDecision.retry(cl)
    } else {
      RetryDecision.rethrow()
    }
  }
  
  override def init(cluster: com.datastax.driver.core.Cluster): Unit = {}
  override def close(): Unit = { }

  override def onReadTimeout(stmt: Statement, cl: ConsistencyLevel,
                             requiredResponses: Int, receivedResponses: Int,
                             dataRetrieved: Boolean, nbRetry: Int) = retryOrThrow(cl, nbRetry)

  override def onUnavailable(stmt: Statement, cl: ConsistencyLevel,
                             requiredReplica: Int, aliveReplica: Int, nbRetry: Int) = retryOrThrow(cl, nbRetry)

  override def onWriteTimeout(stmt: Statement, cl: ConsistencyLevel, writeType: WriteType,
                              requiredAcks: Int, receivedAcks: Int, nbRetry: Int) = retryOrThrow(cl, nbRetry)

}
