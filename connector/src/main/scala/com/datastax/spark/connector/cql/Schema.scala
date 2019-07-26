package com.datastax.spark.connector.cql

import java.io.IOException

import com.datastax.oss.driver.api.core.{CqlIdentifier, ProtocolVersion}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metadata.schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.mapper.{ColumnMapper, DataFrameColumnMapper}
import com.datastax.spark.connector.types.{ColumnType, CounterType}
import com.datastax.spark.connector.util.DriverUtil.{toName, toOption}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{Logging, NameTools}
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.{Properties, Try}

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


sealed trait ColumnRole

case object PartitionKeyColumn extends ColumnRole

case class ClusteringColumn(index: Int) extends ColumnRole

case object StaticColumn extends ColumnRole

case object RegularColumn extends ColumnRole

/** A Cassandra column metadata that can be serialized.  */
case class ColumnDef(
    columnName: String,
    columnRole: ColumnRole,
    columnType: ColumnType[_]) extends FieldDef {

  def ref: ColumnRef = ColumnName(columnName)

  def isStatic = columnRole == StaticColumn

  def isCollection = columnType.isCollection

  def isFrozen = columnType.isFrozen

  def isMultiCell = columnType.isMultiCell

  def isPartitionKeyColumn = columnRole == PartitionKeyColumn

  def isClusteringColumn = columnRole.isInstanceOf[ClusteringColumn]

  def isPrimaryKeyColumn = isClusteringColumn || isPartitionKeyColumn

  def isCounterColumn = columnType == CounterType

  def componentIndex = columnRole match {
    case ClusteringColumn(i) => Some(i)
    case _ => None
  }

  def cql = {
    s"${quote(columnName)} ${columnType.cqlTypeName}"
  }
}

/** Cassandra Index Metadata that can be serialized
  *
  * @param className If this index is custom, the name of the server-side implementation. Otherwise, empty.
  */
case class IndexDef(
    className: Option[String],
    target: String,
    indexName: String,
    options: Map[String, String]) extends Serializable

object ColumnDef {

  def apply(
      column: ColumnMetadata,
      columnRole: ColumnRole): ColumnDef = {

    val columnType = ColumnType.fromDriverType(column.getType)
    ColumnDef(toName(column.getName), columnRole, columnType)
  }
}

/** A Cassandra table metadata that can be serialized. */
case class TableDef(
    keyspaceName: String,
    tableName: String,
    partitionKey: Seq[ColumnDef],
    clusteringColumns: Seq[ColumnDef],
    regularColumns: Seq[ColumnDef],
    indexes: Seq[IndexDef] = Seq.empty,
    isView: Boolean = false) extends StructDef {

  require(partitionKey.forall(_.isPartitionKeyColumn), "All partition key columns must have role PartitionKeyColumn")
  require(clusteringColumns.forall(_.isClusteringColumn), "All clustering columns must have role ClusteringColumn")
  require(regularColumns.forall(!_.isPrimaryKeyColumn), "Regular columns cannot have role PrimaryKeyColumn")

  val allColumns = regularColumns ++ clusteringColumns ++ partitionKey

  private val indexesForTarget: Map[String, Seq[IndexDef]] = indexes.groupBy(_.target)

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

  override val name: String = s"$keyspaceName.$tableName"

  lazy val primaryKey: IndexedSeq[ColumnDef] =
    (partitionKey ++ clusteringColumns).toIndexedSeq

  override lazy val columns: IndexedSeq[ColumnDef] =
    (primaryKey ++ regularColumns).toIndexedSeq

  override lazy val columnByName: Map[String, ColumnDef] =
    super.columnByName

  private lazy val columnBylowerCaseName: Map[String, ColumnDef] = columnByName.map (e => (e._1.toLowerCase, e._2))

  def columnByNameIgnoreCase(columnName: String) = {
    columnBylowerCaseName(columnName.toLowerCase)
  }

  def cql = {
    val columnList = columns.map(_.cql).mkString(s",${Properties.lineSeparator}  ")
    val partitionKeyClause = partitionKey.map(_.columnName).map(quote).mkString("(", ", ", ")")
    val clusteringColumnNames = clusteringColumns.map(_.columnName).map(quote)
    val primaryKeyClause = (partitionKeyClause +: clusteringColumnNames).mkString(", ")

    s"""CREATE TABLE ${quote(keyspaceName)}.${quote(tableName)} (
       |  $columnList,
       |  PRIMARY KEY ($primaryKeyClause)
       |)""".stripMargin
  }

  type ValueRepr = CassandraRow

  lazy val rowMetadata = CassandraRowMetadata.fromColumnNames(columnNames)

  def newInstance(columnValues: Any*): CassandraRow = {
    new CassandraRow(rowMetadata, columnValues.asInstanceOf[IndexedSeq[AnyRef]])
  }
}

object TableDef {

  /** Constructs a table definition based on the mapping provided by
    * appropriate [[com.datastax.spark.connector.mapper.ColumnMapper]] for the given type. */
  def fromType[T: ColumnMapper](
      keyspaceName: String,
      tableName: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): TableDef =
    implicitly[ColumnMapper[T]].newTable(keyspaceName, tableName, protocolVersion)

  def fromDataset(
      dataset: Dataset[_],
      keyspaceName: String,
      tableName: String,
      protocolVersion: ProtocolVersion): TableDef =

    new DataFrameColumnMapper(dataset.schema).newTable(keyspaceName, tableName, protocolVersion)
}

/** A Cassandra keyspace metadata that can be serialized. */
case class KeyspaceDef(keyspaceName: String, tables: Set[TableDef], isSystem: Boolean) {
  lazy val tableByName = tables.map(t => (t.tableName, t)).toMap
}

case class Schema(keyspaces: Set[KeyspaceDef]) {

  /** Returns a map from keyspace name to keyspace metadata */
  lazy val keyspaceByName: Map[String, KeyspaceDef] =
    keyspaces.map(k => (k.keyspaceName, k)).toMap

  /** All tables from all keyspaces */
  lazy val tables: Set[TableDef] =
    for (keyspace <- keyspaces; table <- keyspace.tables) yield table
}

object Schema extends Logging {

  private def fetchPartitionKey(table: RelationMetadata): Seq[ColumnDef] = {
    for (column <- table.getPartitionKey.asScala) yield ColumnDef(column, PartitionKeyColumn)
  }

  private def fetchClusteringColumns(table: RelationMetadata): Seq[ColumnDef] = {
    for ((column, index) <- table.getClusteringColumns.asScala.toSeq.zipWithIndex) yield {
      ColumnDef(column._1, ClusteringColumn(index))
    }
  }

  private def fetchRegularColumns(table: RelationMetadata): Seq[ColumnDef] = {
    val primaryKey = table.getPrimaryKey.asScala.toSet
    val regularColumns = table.getColumns.asScala.values.toSeq.filterNot(primaryKey.contains)
    for (column <- regularColumns) yield {
      if (column.isStatic)
        ColumnDef(column, StaticColumn)
      else
        ColumnDef(column, RegularColumn)
    }
  }

  /** Fetches database schema from Cassandra. Provides access to keyspace, table and column metadata.
    *
    * @param keyspaceName if defined, fetches only metadata of the given keyspace
    * @param tableName    if defined, fetches only metadata of the given table
    */
  def fromCassandra(
      connector: CassandraConnector,
      keyspaceName: Option[String] = None,
      tableName: Option[String] = None): Schema = {

    def isKeyspaceSelected(keyspace: KeyspaceMetadata): Boolean =
      keyspaceName match {
        case None => true
        case Some(name) => toName(keyspace.getName) == name
      }

    def isTableSelected(table: RelationMetadata): Boolean =
      tableName match {
        case None => true
        case Some(name) => toName(table.getName) == name
      }

    def fetchTables(keyspace: KeyspaceMetadata): Set[TableDef] =
      for ((_, table) <- (keyspace.getTables.asScala.toSet ++ keyspace.getViews.asScala.toSet)
           if isTableSelected(table)) yield {
        val partitionKey = fetchPartitionKey(table)
        val clusteringColumns = fetchClusteringColumns(table)
        val regularColumns = fetchRegularColumns(table)
        val indexDefs = getIndexDefs(table)

        val isView = table match {
          case _: ViewMetadata => true
          case _ => false
        }

        TableDef(
          toName(keyspace.getName),
          toName(table.getName),
          partitionKey,
          clusteringColumns,
          regularColumns,
          indexDefs,
          isView)
      }

    def fetchKeyspaces(metadata: Metadata, systemKeyspaces: Set[String]): Set[KeyspaceDef] =
      for ((_, keyspace) <- metadata.getKeyspaces.asScala.toSet if isKeyspaceSelected(keyspace)) yield
        KeyspaceDef(toName(keyspace.getName), fetchTables(keyspace), systemKeyspaces.contains(toName(keyspace.getName)))

    def handleId(table: TableMetadata, columnName: String): String =
      Option(table.getColumn(CqlIdentifier.fromInternal(columnName)))
        .flatMap(toOption)
        .map(c => toName(c.getName))
        .getOrElse(columnName)

    def getIndexDefs(tableOrView: RelationMetadata): Seq[IndexDef] = tableOrView match {
      case table: TableMetadata =>
        for (index <- table.getIndexes.asScala.values.toSeq) yield {
          val className = toOption(index.getClassName)
          val target = handleId(table, index.getTarget)
          IndexDef(className, target, toName(index.getName), Map.empty)
        }
      case _: ViewMetadata => Seq.empty
    }

    connector.withSessionDo { session =>
      logDebug(s"Retrieving database schema")
      val systemKeyspaceNames = Set.empty[String]// TODO FIX THIS DriverUtil.getSystemKeyspaces(session)

      def fetchSchema(metadata: => Metadata): Schema =
        Schema(fetchKeyspaces(metadata, systemKeyspaceNames))

      val schemeStream = fetchSchema(session.getMetadata) #:: fetchSchema(session.refreshSchema()) #:: Stream.empty
      val scheme = schemeStream.find(s => s.tables.nonEmpty).getOrElse(schemeStream.head)

      logDebug(s"${scheme.keyspaces.size} keyspaces fetched: " +
        s"${scheme.keyspaces.map(_.keyspaceName).mkString("{", ",", "}")}")
      scheme
    }
  }


  /**
    * Fetches a TableDef for a particular Cassandra Table throws an
    * exception with name options if the table is not found.
    */
  def tableFromCassandra(
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String): TableDef = {

    fromCassandra(connector, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t) => t
      case None =>
        val metadata: Metadata = connector.withSessionDo(_.getMetadata)
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
        val errorMessage = NameTools.getErrorString(keyspaceName, tableName, suggestions)
        throw new IOException(errorMessage)
    }
  }
}
