package com.datastax.spark.connector

import com.datastax.dse.driver.api.core.auth.ProxyAuthentication
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.oss.driver.api.core.metadata.schema.{KeyspaceMetadata, TableMetadata}
import com.datastax.spark.connector.cql.{CassandraConnector, Schema, TableDef}

/** Useful stuff that didn't fit elsewhere. */
package object util {

  def maybeExecutingAs[StatementT <: Statement[StatementT]](stmt: StatementT, proxyUser: Option[String]): StatementT = {
    proxyUser match {
      case Some(user) =>
        ProxyAuthentication.executeAs(user, stmt)
      case _ =>
        stmt
    }
  }

  def keyspaceFromCassandra(
      connector: CassandraConnector,
      keyspaceName: String): KeyspaceMetadata = {
    connector.withSessionDo(Schema.keyspaceFromCassandra(_, keyspaceName))
  }

  def tableFromCassandra(
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String): TableMetadata = {
    connector.withSessionDo(Schema.tableFromCassandra(_, keyspaceName, tableName))
  }

}
