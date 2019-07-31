/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.spark

//TODO: Commented out to fix later
/*

import org.apache.spark.SparkConf

import com.datastax.bdp.cassandra.auth.DseJavaDriverAuthProvider
import com.datastax.bdp.config.DetachedClientConfigurationFactory
import com.datastax.driver.core.AuthProvider
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider
import com.datastax.spark.connector.cql.{AuthConf, AuthConfFactory, NoAuthConf}
import com.datastax.spark.connector.util.Logging

object DseAuthConfFactory extends AuthConfFactory with Logging {
  val CassandraUserNameProperty = "spark.cassandra.auth.username"
  val CassandraPasswordProperty = "spark.cassandra.auth.password"

  override def properties: Set[String] = Set(
    CassandraUserNameProperty,
    CassandraPasswordProperty
  )

  override def authConf(conf: SparkConf): AuthConf = {
    val clientConf = DetachedClientConfigurationFactory.getClientConfiguration()

    val credentials =
      for (username <- conf.getOption(CassandraUserNameProperty);
           password <- conf.getOption(CassandraPasswordProperty)) yield (username, password)

    // the order of checking which auth conf to use is important - if the user explicitly provided username and
    // password we assume he meant to use plain authentication protocol
    credentials match {
      case Some((user, password)) => DsePasswordAuthConf(user, password)
      case _ if clientConf.isKerberosEnabled => DseAnalyticsKerberosAuthConf
      case _ => NoAuthConf
    }
  }

  object DseAnalyticsKerberosAuthConf extends AuthConf {
    override def authProvider: AuthProvider = {
      new DseJavaDriverAuthProvider()
    }
  }

  case class DsePasswordAuthConf(user: String, password: String) extends AuthConf {
    override def authProvider = new DsePlainTextAuthProvider(user, password)
  }

  def isDelegationTokenBased(conf: AuthConf): Boolean = conf match {
    case DseAnalyticsKerberosAuthConf => true
    case _ => false
  }
}

 */
