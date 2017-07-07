package com.datastax.spark.connector.rdd.partitioner

import java.nio.ByteBuffer

import com.datastax.driver.core.{MetadataHook, Token}
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.util.PatitionKeyTools._
import com.datastax.spark.connector.writer.{BoundStatementBuilder, RoutingKeyGenerator, RowWriter}
import com.datastax.spark.connector.util.Logging

/**
  * A utility class for determining the token of a given key. Uses a bound statement to determine
  * the routing key and the uses that with the TokenFactory to determine the hashed Token.
  */
private[connector] class TokenGenerator[T] (
  connector: CassandraConnector,
  tableDef: TableDef,
  rowWriter: RowWriter[T]) extends Serializable with Logging {

  val protocolVersion = connector.withSessionDo { session =>
    session.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
  }

  //Makes a PreparedStatement which we use only to generate routing keys on the client
  val stmt = connector.withSessionDo { session => prepareDummyStatement(session, tableDef) }
  val metadata = connector.withClusterDo(_.getMetadata)

  val routingKeyGenerator = new RoutingKeyGenerator(
    tableDef,
    tableDef.partitionKey.map(_.columnName))

  val boundStmtBuilder = new BoundStatementBuilder(
    rowWriter,
    stmt,
    protocolVersion = protocolVersion)

  def getPartitionKeyBufferFor(key: T): ByteBuffer = {
    routingKeyGenerator.apply(boundStmtBuilder.bind(key))
  }

  def getTokenFor(key: T): Token = {
    MetadataHook.newToken(metadata, getPartitionKeyBufferFor(key))
  }
}
