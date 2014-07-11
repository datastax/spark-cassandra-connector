package com.datastax.spark.connector.cql

import com.datastax.driver.core.{ColumnMetadata, Metadata, TableMetadata, KeyspaceMetadata}
import com.datastax.spark.connector.types.ColumnType

import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.language.existentials

sealed trait ColumnRole
case object PartitionKeyColumn extends ColumnRole
case class ClusteringColumn(index: Int) extends ColumnRole
case object StaticColumn extends ColumnRole
case object RegularColumn extends ColumnRole

/** A Cassandra column metadata that can be serialized. */
case class ColumnDef(keyspaceName: String,
                     tableName: String,
                     columnName: String,
                     columnRole: ColumnRole,
                     columnType: ColumnType[_]) {

  def isStatic = columnRole == StaticColumn
  def isCollection = columnType.isCollection
  def isPartitionKeyColumn = columnRole == PartitionKeyColumn
  def isClusteringColumn = columnRole.isInstanceOf[ClusteringColumn]
  def isPrimaryKeyColumn = isClusteringColumn || isPartitionKeyColumn

  def componentIndex = columnRole match {
    case ClusteringColumn(i) => Some(i)
    case _ => None
  }
}

/** A Cassandra table metadata that can be serialized. */
case class TableDef(keyspaceName: String,
                    tableName: String,
                    partitionKey: Iterable[ColumnDef], 
                    clusteringKey: Iterable[ColumnDef], 
                    regularColumns: Iterable[ColumnDef]) {
  
  lazy val primaryKey = partitionKey ++ clusteringKey 
  lazy val allColumns = primaryKey ++ regularColumns
  lazy val columnByName = allColumns.map(c => (c.columnName, c)).toMap
}

/** A Cassandra keyspace metadata that can be serialized. */
case class KeyspaceDef(keyspaceName: String, tables: Iterable[TableDef])

/** Fetches database schema from Cassandra. Provides access to keyspace, table and column metadata.
  * @param keyspaceName if defined, fetches only metadata of the given keyspace
  * @param tableName if defined, fetches only metadata of the given table
  */
class Schema(connector: CassandraConnector, keyspaceName: Option[String] = None, tableName: Option[String] = None) extends Logging {

  private val systemKeyspaces = Set("system", "system_traces", "dse_system", "dse_security", "cfs", "cfs_archive", "system_auth")

  private def isKeyspaceSelected(keyspace: KeyspaceMetadata): Boolean =
    keyspaceName match {
      case None => true
      case Some(name) => keyspace.getName == name
    }

  private def isTableSelected(table: TableMetadata): Boolean =
    tableName match {
      case None => true
      case Some(name) => table.getName == name
    }

  private def toColumnDef(column: ColumnMetadata, columnRole: ColumnRole): ColumnDef = {
    val table = column.getTable
    val keyspace = table.getKeyspace
    val columnType = ColumnType.fromDriverType(column.getType)
    ColumnDef(keyspace.getName, table.getName, column.getName, columnRole, columnType)
  }

  private def fetchPartitionKey(table: TableMetadata): Seq[ColumnDef] =
    for (column <- table.getPartitionKey) yield
      toColumnDef(column, PartitionKeyColumn)

  private def fetchClusteringColumns(table: TableMetadata): Seq[ColumnDef] =
    for ((column, index) <- table.getClusteringColumns.zipWithIndex) yield
      toColumnDef(column, ClusteringColumn(index))

  private def fetchRegularColumns(table: TableMetadata) = {
    val primaryKey = table.getPrimaryKey.toSet
    val regularColumns = table.getColumns.filterNot(primaryKey.contains)
    for (column <- regularColumns) yield
      if (column.isStatic)
        toColumnDef(column, StaticColumn)
      else
        toColumnDef(column, RegularColumn)
  }

  private def fetchTables(keyspace: KeyspaceMetadata): Seq[TableDef] =
    for (table <- keyspace.getTables.toSeq if isTableSelected(table)) yield {
      val partitionKey = fetchPartitionKey(table)
      val clusteringColumns = fetchClusteringColumns(table)
      val regularColumns = fetchRegularColumns(table)
      TableDef(keyspace.getName, table.getName, partitionKey, clusteringColumns, regularColumns)
    }

  private def fetchKeyspaces(metadata: Metadata): Seq[KeyspaceDef] =
    for (keyspace <- metadata.getKeyspaces if isKeyspaceSelected(keyspace)) yield
      KeyspaceDef(keyspace.getName, fetchTables(keyspace))


  /** All keyspaces in this database, including the system keyspaces */
  lazy val keyspaces: Seq[KeyspaceDef] = {
    connector.withClusterDo { cluster =>
      val clusterName = cluster.getMetadata.getClusterName
      logDebug(s"Retrieving database schema from cluster $clusterName...")
      val keyspaces = fetchKeyspaces(cluster.getMetadata)
      logDebug(s"${keyspaces.size} keyspaces fetched from cluster $clusterName: " +
        s"${keyspaces.map(_.keyspaceName).mkString("{", ",", "}")}")
      keyspaces
    }
  }

  /** All tables from all keyspaces */
  lazy val tables: Seq[TableDef] =
    for (keyspace <- keyspaces; table <- keyspace.tables) yield table

  /** List of keyspaces created by the user, i.e. non-system keyspaces */
  lazy val userKeyspaces: Seq[KeyspaceDef] =
    keyspaces.filterNot(ks => isSystemKeyspace(ks.keyspaceName))

  /** List of tables created by the user, i.e. non-system tables */
  lazy val userTables: Seq[TableDef] =
    tables.filterNot(tableDef => isSystemKeyspace(tableDef.keyspaceName))

  def isSystemKeyspace(ksName: String) =
    systemKeyspaces.contains(ksName)
}
