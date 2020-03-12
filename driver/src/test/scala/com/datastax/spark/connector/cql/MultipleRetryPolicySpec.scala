package com.datastax.spark.connector.cql

import com.datastax.oss.driver.api.core.config.{DriverConfig, DriverExecutionProfile}
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.retry.RetryDecision
import com.datastax.oss.driver.api.core.servererrors._
import com.datastax.oss.driver.api.core.{DefaultConsistencyLevel, DriverException}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class MultipleRetryPolicySpec extends FlatSpec with Matchers with MockitoSugar {

  private val statement = SimpleStatement.newInstance("foobuzz")
  private val cl = DefaultConsistencyLevel.THREE

  def throwExceptionInPolicy(
    policy: MultipleRetryPolicy,
    exception: DriverException,
    nbRetry: Int = 1): RetryDecision = {

    exception match {
      case e: ReadTimeoutException => policy.onReadTimeout(
        statement,
        cl,
        3,
        1,
        dataPresent = false,
        nbRetry
      )
      case e: WriteTimeoutException => policy.onWriteTimeout(
        statement,
        cl,
        DefaultWriteType.SIMPLE,
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
      case e: CoordinatorException => policy.onErrorResponse(
        statement,
        e,
        nbRetry
      )
    }
  }

  val node = mock[Node]
  val writeTimeout =  new WriteTimeoutException(node, cl, 3, 1, DefaultWriteType.SIMPLE)
  val readTimeout = new ReadTimeoutException(node, cl, 3, 1, false)
  val unavailableException = new UnavailableException(node, cl, 3, 1)
  val retry = RetryDecision.RETRY_SAME
  val rethrow = RetryDecision.RETHROW

  def createRetryPolicy(retries: Int) = {
    val driverContext = mock[DriverContext]
    val driverConfig = mock[DriverConfig]
    val profile = mock[DriverExecutionProfile]
    when(profile.getInt(org.mockito.Matchers.eq(MultipleRetryPolicy.MaxRetryCount), anyInt())).thenReturn(retries)
    when(driverConfig.getProfile(anyString)).thenReturn(profile)
    when(driverContext.getConfig).thenReturn(driverConfig)
    new MultipleRetryPolicy(driverContext, null)
  }

  "MultipleRetryPolicy" should "retry always if maxRetry is -1" in {
    val policy = createRetryPolicy(-1)
    for (nbRetry <- 1 to 100) {
      throwExceptionInPolicy(policy, writeTimeout, nbRetry) should be (retry)
      throwExceptionInPolicy(policy, readTimeout, nbRetry) should be (retry)
    }
  }

  it should "not retry past maxRetry" in {
    val policy = createRetryPolicy(5)
    for (nbRetry <- 6 to 10) {
      throwExceptionInPolicy(policy, writeTimeout, nbRetry) should be (rethrow)
      throwExceptionInPolicy(policy, readTimeout, nbRetry) should be (rethrow)
    }
  }

  it should "not retry authentication errors" in {
    val policy = createRetryPolicy(5)

    throwExceptionInPolicy(policy,
      new UnauthorizedException(node, "oops"), 1) should be (rethrow)
  }

  it should "retry all of our accepted exceptions" in {
    val policy = createRetryPolicy(5)
    throwExceptionInPolicy(policy, writeTimeout) should be(retry)
    throwExceptionInPolicy(policy, readTimeout) should be(retry)
    throwExceptionInPolicy(policy, unavailableException) should be(retry)
  }
}
