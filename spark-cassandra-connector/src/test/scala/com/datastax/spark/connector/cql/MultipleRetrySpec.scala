package com.datastax.spark.connector.cql

import java.net.InetSocketAddress

import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement, WriteType}
import com.datastax.driver.core.exceptions._
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class MultipleRetrySpec extends FlatSpec with Matchers {

  private val statement = new SimpleStatement("foobuzz")
  private val cl = ConsistencyLevel.THREE

  def throwExceptionInPolicy(
                              policy: MultipleRetryPolicy,
                              exception: DriverException,
                              nbRetry: Int = 1): RetryDecision.Type = {

    exception match {
      case e: ReadTimeoutException => policy.onReadTimeout(
        statement,
        cl,
        3,
        1,
        false,
        nbRetry
      )
      case e: WriteTimeoutException => policy.onWriteTimeout(
        statement,
        cl,
        WriteType.SIMPLE,
        3,
        1,
        nbRetry
      )
      case e: UnavailableException => policy.onUnavailable(
        statement,
        cl,
        3,
        1,
        nbRetry
      )
      case e: DriverException => policy.onRequestError(
        statement,
        cl,
        e,
        nbRetry
      )
    }
  }.getType

  val writeTimeout =  new WriteTimeoutException(cl, WriteType.SIMPLE,3, 1)
  val readTimeout = new ReadTimeoutException(cl, 3, 1, false)
  val unavailableException = new UnavailableException(cl, 3, 1)
  val retry = RetryDecision.retry(null).getType
  val rethrow = RetryDecision.rethrow().getType

  "MultipleRetryPolicy" should "retry always if maxRetry is -1" in {
    val policy = new MultipleRetryPolicy(-1)
    for (nbRetry <- 1 to 100) {
      throwExceptionInPolicy(policy, writeTimeout, nbRetry) should be (retry)
      throwExceptionInPolicy(policy, readTimeout, nbRetry) should be (retry)
    }
  }

  it should "not retry past maxRetry" in {
    val policy = new MultipleRetryPolicy(5)
    for (nbRetry <- 6 to 10) {
      throwExceptionInPolicy(policy, writeTimeout, nbRetry) should be (rethrow)
      throwExceptionInPolicy(policy, readTimeout, nbRetry) should be (rethrow)
    }
  }

  it should "not retry authentication errors" in {
    val policy = new MultipleRetryPolicy(5)
    throwExceptionInPolicy(policy,
      new AuthenticationException(new InetSocketAddress(400),"oops"),
      1) should be (rethrow)
  }

  it should "retry all of our accepted exceptions" in {
    val policy = new MultipleRetryPolicy(5)
    throwExceptionInPolicy(policy, writeTimeout) should be(retry)
    throwExceptionInPolicy(policy, readTimeout) should be(retry)
    throwExceptionInPolicy(policy, unavailableException) should be(retry)
  }
}
