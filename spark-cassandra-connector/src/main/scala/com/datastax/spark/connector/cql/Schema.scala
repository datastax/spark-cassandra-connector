package com.datastax.spark.connector.cql

import com.datastax.spark.connector._
import com.datastax.spark.connector.mapper.ColumnMapper
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.language.existentials
import com.datastax.driver.core.{ColumnMetadata, Metadata, TableMetadata, KeyspaceMetadata}
import com.datastax.spark.connector.types.{CounterType, ColumnType}
import com.datastax.spark.connector.util.Quote._

/** Abstract column / field definition.
  * Common to tables and user-defined types */
trait FieldDef extends Serializable {
  def ref: ColumnRef
  def columnName: String
  def columnType: ColumnType[_]
}

/** Cassandra structure that contains columnar information, e.g. a table or a user defined type.
  * This trait allows `ColumnMapper` to work on tables and user defined types.
  * Cassandra tables and user defined types are similar in a way data are extracted from them,
  * therefore a common interface to describe their metadata is handy. */
trait StructDef extends Serializable {

  /** Allows to specify concrete type of column in subclasses,
    * so that `columns` and `columnByName` members return concrete types.
    * Columns in tables may carry more information than columns in user defined types. */
  type Column <: FieldDef

  /** Human-readable name for easy identification of this structure.
    * Used in the error message when the column is not found.
    * E.g. a table name or a type name. */
  def name: String

  /** Sequence of column definitions in this data structure.
    * The order of the columns is implementation-defined. */
  def columns: IndexedSeq[Column]

  /** References to the columns */
  lazy val columnRefs: IndexedSeq[ColumnRef] =
    columns.map(_.ref)

  /** Names of the columns, in the same order as column definitions. */
  def columnNames: IndexedSeq[String] =
    columns.map(_.columnName)

  /** Types of the columns, in the same order as column names and column definitions. */
  def columnTypes: IndexedSeq[ColumnType[_]] =
    columns.map(_.columnType)

  /** For quickly finding a column definition by name.
    * If column is not found, throws NoSuchElementException with information
    * about the name of the column and name of the structure. */
  def columnByName: Map[String, Column] =
    columns.map(c => (c.columnName, c)).toMap.withDefault {
      columnName => throw new NoSuchElementException(s"Column $columnName not found in $name")
    }

  /** For quickly finding a column definition by index.
    * If column is not found, throws NoSuchElementException with information
    * about the requested index of the column and name of the structure. */
  def columnByIndex(index: Int): Column = {
    require(index >= 0 && index < columns.length, s"Column index $index out of bounds for $name")
    columns(index)
  }

  /** Returns the columns that are not present in the structure. */
  def missingColumns(columnsToCheck: Seq[ColumnRef]): Seq[ColumnRef] =
    for (c <- columnsToCheck if !columnByName.contains(c.columnName)) yield c
}


sealed trait ColumnRole
case object PartitionKeyColumn extends ColumnRole
case class ClusteringColumn(index: Int) extends ColumnRole
case object StaticColumn extends ColumnRole
case object RegularColumn extends ColumnRole

/** A Cassandra column metadata that can be serialized. */
case class ColumnDef(
    columnName: String,
    columnRole: ColumnRole,
    columnType: ColumnType[_],
    indexed : Boolean = false) extends FieldDef {

  def ref: ColumnRef = ColumnName(columnName)
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
case class TableDef(
    keyspaceName: String,
    tableName: String,
    partitionKey: Seq[ColumnDef],
    clusteringColumns: Seq[ColumnDef],
    regularColumns: Seq[ColumnDef]) extends StructDef {

  require(partitionKey.forall(_.isPartitionKeyColumn), "All partition key columns must have role PartitionKeyColumn")
  require(clusteringColumns.forall(_.isClusteringColumn), "All clustering columns must have role ClusteringColumn")
  require(regularColumns.forall(!_.isPrimaryKeyColumn), "Regular columns cannot have role PrimaryKeyColumn")

  override type Column = ColumnDef

  override def name: String = s"$keyspaceName.$tableName"
  
  lazy val primaryKey: IndexedSeq[ColumnDef] =
    (partitionKey ++ clusteringColumns).toIndexedSeq

  override lazy val columns: IndexedSeq[ColumnDef] =
    (primaryKey ++ regularColumns).toIndexedSeq

  override lazy val columnByName: Map[String, ColumnDef] =
    super.columnByName

  def cql = {    
    val columnList = columns.map(_.cql).mkString(",\n  ")
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
  def select(selector: ColumnSelector): IndexedSeq[ColumnDef] = {
    selector match {
      case AllColumns => columns
      case PartitionKeyColumns => partitionKey
      case SomeColumns(names @ _*) => names.map {
        case ColumnName(columnName, _) =>
          columnByName(columnName)
        case columnRef =>
          throw new IllegalArgumentException(s"Invalid column reference $columnRef for table $keyspaceName.$tableName")
      }
    }
  }.toIndexedSeq
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
