package com.datastax.spark.connector.cql

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.config.DriverOption
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.retry.{RetryDecision, RetryPolicy}
import com.datastax.oss.driver.api.core.servererrors.{CoordinatorException, WriteType}
import com.datastax.oss.driver.api.core.session.Request

/** Always retries with the same CL (null forces the original statement CL see SPARKC-494),
  *  constant number of times, regardless of circumstances
  *
  *  Retries indefinitely if maxRetryCount is -1
  */
class MultipleRetryPolicy(context: DriverContext, profileName: String)
  extends RetryPolicy {

  private val maxRetryCount = context.getConfig.getProfile(profileName).getInt(
    MultipleRetryPolicy.MaxRetryCount, MultipleRetryPolicy.MaxRetryCountDefault)

  private def retryManyTimesOrThrow(nbRetry: Int): RetryDecision = maxRetryCount match {
    case -1 => RetryDecision.RETRY_SAME
    case maxRetries =>
      if (nbRetry < maxRetries) {
        RetryDecision.RETRY_SAME
      } else {
        RetryDecision.RETHROW
      }
  }

  override def onReadTimeout(
    request: Request,
    cl: ConsistencyLevel,
    blockFor: Int,
    received: Int,
    dataPresent: Boolean,
    retryCount: Int): RetryDecision = retryManyTimesOrThrow(retryCount)

  override def onWriteTimeout(
    request: Request,
    cl: ConsistencyLevel,
    writeType: WriteType,
    blockFor: Int,
    received: Int,
    retryCount: Int): RetryDecision = retryManyTimesOrThrow(retryCount)

  override def onUnavailable(
    request: Request,
    cl: ConsistencyLevel,
    required: Int,
    alive: Int,
    retryCount: Int): RetryDecision = retryManyTimesOrThrow(retryCount)

  override def onRequestAborted(
    request: Request,
    error: Throwable,
    retryCount: Int): RetryDecision = retryManyTimesOrThrow(retryCount)

  override def onErrorResponse(
    request: Request,
    error: CoordinatorException,
    retryCount: Int): RetryDecision = RetryDecision.RETHROW

  override def close(): Unit = {}
}

object MultipleRetryPolicy {
  val MaxRetryCount: DriverOption = new DriverOption {
    override def getPath: String = "advanced.retry-policy.max-retry-count"
  }

  val MaxRetryCountDefault: Int = 60
}