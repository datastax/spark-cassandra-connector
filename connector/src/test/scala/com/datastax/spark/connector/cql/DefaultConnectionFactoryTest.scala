package com.datastax.spark.connector.cql

import java.io.IOException

import org.apache.spark.SparkEnv
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FlatSpec
import org.scalatestplus.mockito.MockitoSugar

class DefaultConnectionFactoryTest extends FlatSpec with MockitoSugar {

  /** DefaultConnectionFactory relies on a non-null SparkEnv */
  private def mockedSparkEnv[T](code: => T): T = {
    val original = SparkEnv.get
    val sparkEnv = Mockito.mock(classOf[SparkEnv], new Answer[Option[String]] {
      override def answer(invocation: InvocationOnMock): Option[String] = None
    })
    SparkEnv.set(sparkEnv)
    try {
      code
    } finally {
      SparkEnv.set(original)
    }
  }

  it should "complain when a malformed URL is provided" in mockedSparkEnv {
    intercept[IOException] {
      DefaultConnectionFactory.maybeGetLocalFile("secure-bundle.zip")
    }
  }

  it should "complain when an URL with unrecognized scheme is provided" in mockedSparkEnv {
    intercept[IOException] {
      DefaultConnectionFactory.maybeGetLocalFile("hdfs:///secure-bundle.zip")
    }
  }

}
