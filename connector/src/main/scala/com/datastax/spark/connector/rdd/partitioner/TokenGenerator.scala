package com.datastax.spark.connector.rdd.partitioner

import java.nio.ByteBuffer

import com.datastax.driver.core.MetadataHook
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.spark.connector.cql.{CassandraConnector, QueryUtils, TableDef}
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.util.PatitionKeyTools._
import com.datastax.spark.connector.writer.{BoundStatementBuilder, NullKeyColumnException, RowWriter}

import scala.collection.JavaConverters._

/**
  * A utility class for determining the token of a given key. Uses a bound statement to determine
  * the routing key and the uses that with the TokenFactory to determine the hashed Token.
  */
private[connector] class TokenGenerator[T] (
  connector: CassandraConnector,
  tableDef: TableDef,
  rowWriter: RowWriter[T]) extends Serializable with Logging {

  val protocolVersion = connector.withSessionDo { session =>
    session.getContext.getProtocolVersion
  }

  //Makes a PreparedStatement which we use only to generate routing keys on the client
  val stmt = connector.withSessionDo { session => prepareDummyStatement(session, tableDef) }
  val metadata = connector.withSessionDo(_.getMetadata)

  val boundStmtBuilder = new BoundStatementBuilder(
    rowWriter,
    stmt,
    protocolVersion = protocolVersion)

  def getRoutingKey(key: T): ByteBuffer = {
    val boundStatement = boundStmtBuilder.bind(key).stmt
    QueryUtils.getRoutingKeyOrError(boundStatement)
  }

  def getTokenFor(key: T): Token = {
    MetadataHook.newToken(metadata, getRoutingKey(key))
  }

  def getStringTokenFor(key: T): String = {
    MetadataHook.newTokenAsString(metadata, getRoutingKey(key))
  }
}
