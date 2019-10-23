/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.spark

import com.datastax.spark.connector.cql.{AuthConfFactory, DefaultAuthConfFactory}
import com.datastax.spark.connector.util.{ConfigCheck, ConfigParameter}

object DseConnectionParams {

  val SparkCassandraPrefix = "spark.cassandra."
  val DseFsPortProperty = "dsefs.port"

  val UserNameParam = DefaultAuthConfFactory.UserNameParam

  val PasswordParam = DefaultAuthConfFactory.PasswordParam

  val SaslProtocolParam = ConfigParameter[String](
    name = "spark.cassandra.auth.saslProtocol",
    section = AuthConfFactory.ReferenceSection,
    default = sys.props.getOrElse("dse.sasl.protocol", "dse"),
    description = "SASL protocol name - for DSE it is the service principal short name used on the server (usually 'dse')")

  val QoPParam = ConfigParameter[String](
    name = "spark.cassandra.auth.qop",
    section = AuthConfFactory.ReferenceSection,
    default = sys.props.getOrElse("cassandra.sasl.qop", "auth"),
    description = s"Quality of protection to be used with SASL based authentication protocol (for example Kerberos) - can be: auth, int, conf")

  val AuthTokenParam = ConfigParameter[String](
    name = "spark.cassandra.auth.token",
    section = AuthConfFactory.ReferenceSection,
    default = "",
    description = "Delegation token for SASL based authentication")

  val ServerAuthEnabledParam = ConfigParameter[Boolean](
    name = "spark.cassandra.auth.serverAuthEnabled",
    section = AuthConfFactory.ReferenceSection,
    default = true,
    description = "Authenticate server when using SASL based authentication protocol")

  val DseFsPortParam = ConfigParameter[Int](
    name = SparkCassandraPrefix + DseFsPortProperty,
    section = "",
    default = 5598,
    description = "DSE FS port")
}
