package com.datastax.spark.connector.cql

import com.datastax.spark.connector._
import com.datastax.spark.connector.mapper.ColumnMapper
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.language.existentials
import com.datastax.driver.core.{ColumnMetadata, Metadata, TableMetadata, KeyspaceMetadata}
import com.datastax.spark.connector.types.{CounterType, ColumnType}

sealed trait ColumnRole
case object PartitionKeyColumn extends ColumnRole
case class ClusteringColumn(index: Int) extends ColumnRole
case object StaticColumn extends ColumnRole
case object RegularColumn extends ColumnRole

/** A Cassandra column metadata that can be serialized. */
case class ColumnDef(columnName: String,
                     columnRole: ColumnRole,
                     columnType: ColumnType[_],
                     indexed : Boolean = false) {

  def isStatic = columnRole == StaticColumn
  def isCollection = columnType.isCollection
  def isPartitionKeyColumn = columnRole == PartitionKeyColumn
  def isClusteringColumn = columnRole.isInstanceOf[ClusteringColumn]
  def isPrimaryKeyColumn = isClusteringColumn || isPartitionKeyColumn
  def isCounterColumn = columnType == CounterType
  def isIndexedColumn = indexed

  def componentIndex = columnRole match {
    case ClusteringColumn(i) => Some(i)
    case _ => None
  }

  def cql = {
    def quote(str: String) = "\"" + str + "\""
    s"${quote(columnName)} ${columnType.cqlTypeName}"
  }
}

object ColumnDef {

  def apply(column: ColumnMetadata, columnRole: ColumnRole): ColumnDef = {
    val columnType = ColumnType.fromDriverType(column.getType)
    ColumnDef(column.getName, columnRole, columnType, column.getIndex != null)
  }
}

/** A Cassandra table metadata that can be serialized. */
case class TableDef(keyspaceName: String,
                    tableName: String,
                    partitionKey: Seq[ColumnDef],
                    clusteringColumns: Seq[ColumnDef],
                    regularColumns: Seq[ColumnDef]) {

  require(partitionKey.forall(_.isPartitionKeyColumn), "All partition key columns must have role PartitionKeyColumn")
  require(clusteringColumns.forall(_.isClusteringColumn), "All clustering columns must have role ClusteringColumn")
  require(regularColumns.forall(!_.isPrimaryKeyColumn), "Regular columns cannot have role PrimaryKeyColumn")

  lazy val primaryKey = partitionKey ++ clusteringColumns
  lazy val allColumns = primaryKey ++ regularColumns
  lazy val columnByName = allColumns.map(c => (c.columnName, c))
    .toMap.withDefault {
      name => throw new NoSuchElementException(s"Column not found $name in table $keyspaceName.$tableName")
    }

  def cql = {
    def quote(str: String) = "\"" + str + "\""
    val columnList = allColumns.map(_.cql).mkString(",\n  ")
    val partitionKeyClause = partitionKey.map(_.columnName).map(quote).mkString("(", ", ", ")")
    val clusteringColumnNames = clusteringColumns.map(_.columnName).map(quote)
    val primaryKeyClause = (partitionKeyClause +: clusteringColumnNames).mkString(", ")

    s"""CREATE TABLE ${quote(keyspaceName)}.${quote(tableName)} (
       |  $columnList,
       |  PRIMARY KEY ($primaryKeyClause)
       |)""".stripMargin
  }

  /** Selects a subset of columns.
    * Columns are returned in the order specified in the `ColumnSelector`. */
  def select(columns: ColumnSelector): Seq[ColumnDef] = {
    columns match {
      case AllColumns => allColumns
      case PartitionKeyColumns => partitionKey
      case SomeColumns(names @ _*) => names.map {
        case ColumnName(columnName, _) =>
          columnByName(columnName)
        case columnRef =>
          throw new IllegalArgumentException(s"Invalid column reference $columnRef for table $keyspaceName.$tableName")
      }
    }
  }
}

object TableDef {

  /** Constructs a table definition based on the mapping provided by
    * appropriate [[com.datastax.spark.connector.mapper.ColumnMapper]] for the given type. */
  def fromType[T : ColumnMapper](keyspaceName: String, tableName: String): TableDef =
    implicitly[ColumnMapper[T]].newTable(keyspaceName, tableName)
}

/** A Cassandra keyspace metadata that can be serialized. */
case class KeyspaceDef(keyspaceName: String, tables: Set[TableDef]) {
  lazy val tableByName = tables.map(t => (t.tableName, t)).toMap
}

case class Schema(clusterName: String, keyspaces: Set[KeyspaceDef]) {

  /** Returns a map from keyspace name to keyspace metadata */
  lazy val keyspaceByName: Map[String, KeyspaceDef] =
    keyspaces.map(k => (k.keyspaceName, k)).toMap

  /** All tables from all keyspaces */
  lazy val tables: Set[TableDef] =
    for (keyspace <- keyspaces; table <- keyspace.tables) yield table

}

object Schema extends Logging {

  private def fetchPartitionKey(table: TableMetadata): Seq[ColumnDef] =
    for (column <- table.getPartitionKey) yield
      ColumnDef(column, PartitionKeyColumn)

  private def fetchClusteringColumns(table: TableMetadata): Seq[ColumnDef] =
    for ((column, index) <- table.getClusteringColumns.zipWithIndex) yield
      ColumnDef(column, ClusteringColumn(index))

  private def fetchRegularColumns(table: TableMetadata) = {
    val primaryKey = table.getPrimaryKey.toSet
    val regularColumns = table.getColumns.filterNot(primaryKey.contains)
    for (column <- regularColumns) yield
      if (column.isStatic)
        ColumnDef(column, StaticColumn)
      else
        ColumnDef(column, RegularColumn)
  }

  /** Fetches database schema from Cassandra. Provides access to keyspace, table and column metadata.
    * @param keyspaceName if defined, fetches only metadata of the given keyspace
    * @param tableName if defined, fetches only metadata of the given table
    */
  def fromCassandra(connector: CassandraConnector, keyspaceName: Option[String] = None, tableName: Option[String] = None): Schema = {

    def isKeyspaceSelected(keyspace: KeyspaceMetadata): Boolean =
      keyspaceName match {
        case None => true
        case Some(name) => keyspace.getName == name
      }

    def isTableSelected(table: TableMetadata): Boolean =
      tableName match {
        case None => true
        case Some(name) => table.getName == name
      }

    def fetchTables(keyspace: KeyspaceMetadata): Set[TableDef] =
      for (table <- keyspace.getTables.toSet if isTableSelected(table)) yield {
        val partitionKey = fetchPartitionKey(table)
        val clusteringColumns = fetchClusteringColumns(table)
        val regularColumns = fetchRegularColumns(table)
        TableDef(keyspace.getName, table.getName, partitionKey, clusteringColumns, regularColumns)
      }

    def fetchKeyspaces(metadata: Metadata): Set[KeyspaceDef] =
      for (keyspace <- metadata.getKeyspaces.toSet if isKeyspaceSelected(keyspace)) yield
        KeyspaceDef(keyspace.getName, fetchTables(keyspace))

    connector.withClusterDo { cluster =>
      val clusterName = cluster.getMetadata.getClusterName
      logDebug(s"Retrieving database schema from cluster $clusterName...")
      val keyspaces = fetchKeyspaces(cluster.getMetadata)
      logDebug(s"${keyspaces.size} keyspaces fetched from cluster $clusterName: " +
        s"${keyspaces.map(_.keyspaceName).mkString("{", ",", "}")}")
      Schema(clusterName, keyspaces)
    }
  }
}
