/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark

import java.util.concurrent.atomic.AtomicReference

import com.datastax.bdp.config.ClientConfiguration
import com.datastax.bdp.util.DseConnectionUtil
import com.datastax.driver.core._
import com.datastax.spark.connector.cql._

/** Provides means to build clusters out of supplied `ClientConfiguration` instead of `CassandraConnectorConf`.
  * As `ClientConfiguration`, it is meant to be used by applications which run in single, separate JVM processes.
  * It mustn't be used in DSE process, nor in Spark Applications. */
private[bdp] object DseConfigurationBasedConnectionFactory extends CassandraConnectionFactory {

  private case class ConnectionFactoryConfig(
      clientConfiguration: ClientConfiguration,
      username: String,
      password: String,
      token: String)

  @transient
  private val connectionFactoryConfig: AtomicReference[ConnectionFactoryConfig] = new AtomicReference()

  def setConnectionConfig(
      clientConfiguration: ClientConfiguration,
      username: String,
      password: String,
      token: String): Unit = {
    val config = ConnectionFactoryConfig(clientConfiguration, username, password, token)
    if (!connectionFactoryConfig.compareAndSet(null, config)) {
      throw new IllegalStateException("Building config cannot be changed once it is set.")
    }
  }

  override def createCluster(conf: CassandraConnectorConf): Cluster =
    Option(connectionFactoryConfig.get())
      .map(config =>
        DseConnectionUtil
          .createClusterBuilder(config.clientConfiguration, config.username, config.password, config.token)
          .withThreadingOptions(new CassandraConnectionFactory.DaemonThreadingOptions)
          .build())
      .getOrElse(throw new IllegalStateException("Building config hasn't been set."))
}
