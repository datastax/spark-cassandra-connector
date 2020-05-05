package com.datastax.spark.connector.cql

import java.io.IOException

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metadata.schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.{ColumnType, CounterType}
import com.datastax.spark.connector.util.DriverUtil.toName
import com.datastax.spark.connector.util.NameTools
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.Try

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
  val name: String

  /** Sequence of column definitions in this data structure.
    * The order of the columns is implementation-defined. */
  val columns: IndexedSeq[Column]

  /** References to the columns */
  lazy val columnRefs: IndexedSeq[ColumnRef] =
    columns.map(_.ref)

  /** Names of the columns, in the same order as column definitions. */
  lazy val columnNames: IndexedSeq[String] =
    columns.map(_.columnName)

  /** Types of the columns, in the same order as column names and column definitions. */
  lazy val columnTypes: IndexedSeq[ColumnType[_]] =
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

  /** Type of the data described by this struct */
  type ValueRepr <: AnyRef

  /** Creates new instance of this struct.
    * Column values must be given in the same order as columnNames */
  def newInstance(columnValues: Any*): ValueRepr
}

case class ColumnDef(column:ColumnMetadata,
                     relation:RelationMetadata,
                     keyspace:KeyspaceMetadata) extends FieldDef {

  def ref: ColumnRef = ColumnName(columnName)
  def columnName = column.getName.toString
  def columnType = ColumnType.fromDriverType(column.getType)

  def isStatic = column.isStatic

  def isCollection = columnType.isCollection

  def isFrozen = columnType.isFrozen

  def isMultiCell = columnType.isMultiCell

  def isPartitionKeyColumn = relation.getPartitionKey.asScala.contains(column)

  def isClusteringColumn = relation.getClusteringColumns.keySet().asScala.contains(column)

  def isPrimaryKeyColumn = isClusteringColumn || isPartitionKeyColumn

  def isCounterColumn = columnType == CounterType

  def sortingDirection = relation.getClusteringColumns.asScala.get(column)
}

case class IndexDef(
    index:IndexMetadata,
    table:TableMetadata,
    keyspace:KeyspaceMetadata) extends Serializable

/** A Cassandra table metadata that can be serialized. */
case class TableDef(
                     relation:RelationMetadata,
                     keyspace:KeyspaceMetadata) extends StructDef {

  val keyspaceName = keyspace.getName.toString
  val tableName = relation.getName.toString
  val cols = relation.getColumns.values().asScala.map(c => ColumnDef(c,relation,keyspace)).toSet
  val partitionKey = cols.filter(c => relation.getPartitionKey.contains(c.column))
  val clusteringColumns = cols.filter(c=> relation.getClusteringColumns.values().contains(c.column))
  val regularColumns = cols -- partitionKey
  val isTable = relation.isInstanceOf[TableMetadata]
  val isView = relation.isInstanceOf[ViewMetadata]
  val asTable = if (isTable) Some(relation.asInstanceOf[TableMetadata]) else Option.empty
  val asView = if (isView) Some(relation.asInstanceOf[ViewMetadata]) else Option.empty

  val indexes =
    asTable match {
      case Some(table:TableMetadata) =>
        table.getIndexes.values().asScala.map(i => IndexDef(i,table, keyspace)).toSeq
      case None => Seq.empty
    }

  val allColumns = regularColumns ++ clusteringColumns ++ partitionKey

  private val indexesForTarget: Map[String, Seq[IndexDef]] = indexes.groupBy(_.index.getTarget)

  /**
    * Contains indices that can be directly mapped to single column, namely indices with a handled column
    * name as a target. Indices that can not be mapped to a single column are dropped.
    */
  private val indexesForColumnDef: Map[ColumnDef, Seq[IndexDef]] = {
    indexesForTarget.flatMap {
      case (target, indexes) => Try(columnByName(target) -> indexes).toOption
    }
  }

  def isIndexed(column: String): Boolean = {
    indexesForTarget.contains(column)
  }

  def isIndexed(column: ColumnDef): Boolean = {
    indexesForColumnDef.contains(column)
  }

  val indexedColumns: Seq[ColumnDef] = {
    indexesForColumnDef.keys.toSeq
  }

  override type Column = ColumnDef

  override val name: String = s"${keyspace.getName}.${relation.getName}"

  lazy val primaryKey: IndexedSeq[ColumnDef] =
    (partitionKey ++ clusteringColumns).toIndexedSeq

  override lazy val columns: IndexedSeq[ColumnDef] =
    (primaryKey ++ regularColumns).toIndexedSeq

  override lazy val columnByName: Map[String, ColumnDef] =
    super.columnByName

  private lazy val columnBylowerCaseName: Map[String, ColumnDef] = columnByName.map(e => (e._1.toLowerCase, e._2))

  def columnByNameIgnoreCase(columnName: String) = {
    columnBylowerCaseName(columnName.toLowerCase)
  }

  def cql = relation.describe(true)

  type ValueRepr = CassandraRow

  lazy val rowMetadata = CassandraRowMetadata.fromColumnNames(columnNames)

  def newInstance(columnValues: Any*): CassandraRow = {
    new CassandraRow(rowMetadata, columnValues.asInstanceOf[IndexedSeq[AnyRef]])
  }
}

case class Schema(keyspaces: Set[KeyspaceMetadata]) {

  /** Returns a map from keyspace name to keyspace metadata */
  lazy val keyspaceByName: Map[CqlIdentifier, KeyspaceMetadata] =
    keyspaces.map(k => (k.getName, k)).toMap

  /** All tables from all keyspaces */
  lazy val tables: Set[TableDef] =
    keyspaces.flatMap(k =>
      (k.getTables.asScala.values ++ k.getViews.asScala.values).map(t => TableDef(t,k))
    )
}

object Schema extends StrictLogging {

  /** Fetches database schema from Cassandra. Provides access to keyspace, table and column metadata.
    *
    * @param keyspaceName if defined, fetches only metadata of the given keyspace
    * @param tableName    if defined, fetches only metadata of the given table
    */
  def fromCassandra(
      session: CqlSession,
      keyspaceName: Option[String] = None,
      tableName: Option[String] = None): Schema = {

    def isKeyspaceSelected(keyspace: KeyspaceMetadata): Boolean =
      keyspaceName match {
        case None => true
        case Some(name) => toName(keyspace.getName) == name
      }

    def fetchKeyspaces(metadata: Metadata): Set[KeyspaceMetadata] =
      metadata.getKeyspaces.values().asScala.filter(isKeyspaceSelected(_)).toSet

    logger.debug(s"Retrieving database schema")
    val schema = Schema(fetchKeyspaces(session.refreshSchema()))
    logger.debug(s"${schema.keyspaces.size} keyspaces fetched: " +
      s"${schema.keyspaces.map(_.getName).mkString("{", ",", "}")}")
    schema
  }


  /**
    * Fetches a TableDef for a particular Cassandra Table throws an
    * exception with name options if the table is not found.
    */
  def tableFromCassandra(
      session: CqlSession,
      keyspaceName: String,
      tableName: String): TableDef = {

    fromCassandra(session, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None =>
        val metadata: Metadata = session.getMetadata
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
        val errorMessage = NameTools.getErrorString(keyspaceName, tableName, suggestions)
        throw new IOException(errorMessage)
    }
  }
}
