package com.datastax.spark.connector.writer


import java.io.IOException
import java.net.InetAddress

import com.datastax.driver.core._
import com.datastax.spark.connector.{ColumnSelector, ColumnRef}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.collection._

/**
 * A utility class for determining the Replica Set (Ip Addresses) of a particular Cassandra Row. Used
 * by the [[com.datastax.spark.connector.RDDFunctions.keyByCassandraReplica]] method. Uses the Java
 * Driver to obtain replica information.
 */
class ReplicaMapper[T] private(
    connector: CassandraConnector,
    tableDef: TableDef,
    rowWriter: RowWriter[T]) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames
  implicit val protocolVersion = connector.withClusterDo {
    _.getConfiguration.getProtocolOptions.getProtocolVersionEnum
  }

  /**
   * This query is only used to build a prepared statement so we can more easily extract
   * partition tokens from tables. We prepare a statement of the form SELECT * FROM keyspace.table
   * where x= .... This statement is never executed.
   */
  private lazy val querySelectUsingOnlyPartitionKeys: String = {
    val partitionKeys = tableDef.partitionKey
    def quotedColumnNames(columns: Seq[ColumnDef]) = partitionKeys.map(_.columnName).map(quote)
    val whereClause = quotedColumnNames(partitionKeys).map(c => s"$c = :$c").mkString(" AND ")
    s"SELECT * FROM ${quote(keyspaceName)}.${quote(tableName)} WHERE $whereClause"
  }

  private def prepareDummyStatement(session: Session): PreparedStatement = {
    try {
      session.prepare(querySelectUsingOnlyPartitionKeys)
    }
    catch {
      case t: Throwable =>
        throw new IOException(s"Failed to prepare statement $querySelectUsingOnlyPartitionKeys: " + t.getMessage, t)
    }
  }

  /**
   * Pairs each piece of data with the Cassandra Replicas which that data would be found on
   * @param data A source of data which can be bound to a statement by BatchStatementBuilder
   * @return an Iterator over the same data keyed by the replica's ip addresses
   */
  def keyByReplicas(data: Iterator[T]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] = {
      connector.withSessionDo { session =>
        val stmt = prepareDummyStatement(session)
        val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
        val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion)
        val clusterMetadata = session.getCluster.getMetadata
        data.map { row =>
          val hosts = clusterMetadata
            .getReplicas(Metadata.quote(keyspaceName), routingKeyGenerator.apply(boundStmtBuilder.bind(row)))
            .map(_.getAddress)
            .toSet[InetAddress]
          (hosts, row)
        }
    }
  }
}

/**
 * Helper methods for mapping a set of data to their relative locations in a Cassandra Cluster.
 */
object ReplicaMapper {
  def apply[T: RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      partitionKeyMapper: ColumnSelector): ReplicaMapper[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef,
      partitionKeyMapper.selectFrom(tableDef)
    )
    new ReplicaMapper[T](connector, tableDef, rowWriter)
  }

}
