package com.datastax.spark.connector.cql

import java.io.IOException

import com.datastax.oss.driver.api.core.{CqlSession, ProtocolVersion}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metadata.schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.types.{ColumnType, CounterType, UserDefinedType}
import com.datastax.spark.connector.util.DriverUtil.{toName, toOption}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{DriverUtil, NameTools}
import com.typesafe.scalalogging.StrictLogging

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

object ClusteringColumn {
  sealed trait SortingOrder
  case object Ascending extends SortingOrder {
    override def toString = "ASC"
  }
  case object Descending extends SortingOrder {
    override def toString = "DESC"
  }
}
case class ClusteringColumn(index: Int, sortDirection: ClusteringColumn.SortingOrder = ClusteringColumn.Ascending)
  extends ColumnRole

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
    case ClusteringColumn(i, _) => Some(i)
    case _ => None
  }

  def sortingDirection = columnRole match {
    case ClusteringColumn(_, v) => v
    case _ => true
  }

  def cql = {
    s"${quote(columnName)} ${columnType.cqlTypeName}"
  }
}

object ColumnDef {

  def apply(
      column: ColumnMetadata,
      columnRole: ColumnRole): ColumnDef = {

    val columnType = ColumnType.fromDriverType(column.getType)
    ColumnDef(toName(column.getName), columnRole, columnType)
  }

  def columnName(implicit arg:ColumnMetadata) = DriverUtil.toName(arg.getName)
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

object IndexDef {

  def indexName(implicit arg:IndexMetadata) = DriverUtil.toName(arg.getName)
}

/** A Cassandra table metadata that can be serialized. */
case class TableDef(
    keyspaceName: String,
    tableName: String,
    partitionKey: Seq[ColumnDef],
    clusteringColumns: Seq[ColumnDef],
    regularColumns: Seq[ColumnDef],
    indexes: Seq[IndexDef] = Seq.empty,
    isView: Boolean = false,
    ifNotExists: Boolean = false,
    tableOptions: Map[String, String] = Map()) extends StructDef {

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

  private lazy val columnBylowerCaseName: Map[String, ColumnDef] = columnByName.map(e => (e._1.toLowerCase, e._2))

  def columnByNameIgnoreCase(columnName: String) = {
    columnBylowerCaseName(columnName.toLowerCase)
  }

  def cql = {
    val columnList = columns.map(_.cql).mkString(s",${Properties.lineSeparator}  ")
    val partitionKeyClause = partitionKey.map(_.columnName).map(quote).mkString("(", ", ", ")")
    val clusteringColumnNames = clusteringColumns.map(_.columnName).map(quote)
    val primaryKeyClause = (partitionKeyClause +: clusteringColumnNames).mkString(", ")
    val addIfNotExists = if (ifNotExists) "IF NOT EXISTS " else ""
    val clusterOrder = if (clusteringColumns.isEmpty ||
      clusteringColumns.forall(x => x.sortingDirection == ClusteringColumn.Ascending)) {
      Seq()
    } else {
      Seq("CLUSTERING ORDER BY (" + clusteringColumns.map(x=> quote(x.columnName) +
          " " + x.sortingDirection).mkString(", ") + ")")
    }
    val allOptions = clusterOrder ++ tableOptions.map(x => x._1 + " = " + x._2)
    val allOptionsStr = if (allOptions.isEmpty)
      ""
    else
      allOptions.mkString(" WITH ", "\n  AND ", "")

    s"""CREATE TABLE $addIfNotExists${quote(keyspaceName)}.${quote(tableName)} (
       |  $columnList,
       |  PRIMARY KEY ($primaryKeyClause)
       |)$allOptionsStr""".stripMargin
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

  def tableName(implicit arg:TableMetadata) = DriverUtil.toName(arg.getName)

  def name(implicit arg: TableMetadata): String =
    s"${DriverUtil.toName(arg.getKeyspace)}.${TableDef.tableName(arg)}"

  def keyspaceName(implicit arg: TableMetadata) = DriverUtil.toName(arg.getKeyspace)

  def columns(implicit arg: TableMetadata): IndexedSeq[ColumnMetadata] =
    arg.getColumns.asScala.values.toIndexedSeq

  def columnByName(colName:String)(implicit arg: TableMetadata): ColumnMetadata =
    arg.getColumn(colName)
      .orElseThrow(() => new NoSuchElementException(s"Column $colName not found in $name"))

  def partitionKey(implicit arg: TableMetadata): Seq[ColumnMetadata] = {
    val partitionKeyNames = arg.getPartitionKey.asScala.map(_.getName).toSet
    columns(arg).filter(c => partitionKeyNames(c.getName))
  }
  def clusteringColumns(implicit arg: TableMetadata): Seq[ColumnMetadata] = {
    val clusteringColumnNames = arg.getClusteringColumns.keySet.asScala.map(_.getName).toSet
    columns(arg).filter(c => clusteringColumnNames(c.getName))
  }

  def primaryKey(implicit arg: TableMetadata): IndexedSeq[ColumnMetadata] =
    (partitionKey(arg) ++ clusteringColumns(arg)).toIndexedSeq

  def regularColumns(implicit arg: TableMetadata):Seq[ColumnMetadata] = columns(arg).filterNot(primaryKey(arg).toSet)

  def indexes(implicit arg: TableMetadata): Seq[IndexMetadata] = arg.getIndexes.asScala.values.toSeq

  def indexesForTarget(implicit arg: TableMetadata): Map[String, Seq[IndexMetadata]] = indexes.groupBy(_.getTarget)

  /**
   * Contains indices that can be directly mapped to single column, namely indices with a handled column
   * name as a target. Indices that can not be mapped to a single column are dropped.
   */
  def indexesForColumnDef(implicit arg: TableMetadata): Map[ColumnMetadata, Seq[IndexMetadata]] = {
    indexesForTarget.flatMap {
      case (target, indexes) => Try(arg.getColumn(target).get() -> indexes).toOption
    }
  }

  def indexedColumns(implicit arg: TableMetadata): Seq[ColumnMetadata] = {
    indexesForColumnDef.keys.toSeq
  }
}

/** A Cassandra keyspace metadata that can be serialized. */
case class KeyspaceDef(keyspaceName: String, tables: Set[TableDef], userTypes: Set[UserDefinedType], isSystem: Boolean) {
  lazy val tableByName: Map[String, TableDef] = tables.map(t => (t.tableName, t)).toMap
  lazy val userTypeByName: Map[String, UserDefinedType] = userTypes.map(t => (t.name, t)).toMap
}

object KeyspaceDef {

  def keyspaceName(implicit arg:KeyspaceMetadata) = DriverUtil.toName(arg.getName)
}

case class Schema(keyspaces: Set[KeyspaceDef]) {

  /** Returns a map from keyspace name to keyspace metadata */
  lazy val keyspaceByName: Map[String, KeyspaceDef] =
    keyspaces.map(k => (k.keyspaceName, k)).toMap

  /** All tables from all keyspaces */
  lazy val tables: Set[TableDef] =
    for (keyspace <- keyspaces; table <- keyspace.tables) yield table
}

object Schema extends StrictLogging {

  /**
   * Fetches a KeyspaceMetadata, throwing an exception with name options if the keyspace is not found.
   */
  def keyspaceFromCassandra(session: CqlSession,
                            keyspaceName: String): KeyspaceMetadata = {

    val tableOption = session
      .getMetadata()
      .getKeyspace(keyspaceName)
    toOption(tableOption) match {
      case Some(t) => t
      case None =>
        val metadata: Metadata = session.getMetadata
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName)
        val errorMessage = NameTools.getErrorString(keyspaceName, None, suggestions)
        throw new IOException(errorMessage)
    }
  }

  /**
    * Fetches a TableMetadata for a particular Cassandra Table, throwing an
    * exception with name options if the table is not found.
    */
  def tableFromCassandra(session: CqlSession,
                         keyspaceName: String,
                         tableName: String): TableMetadata = {

    val tableOption = session
      .getMetadata()
      .getKeyspace(keyspaceName)
      .flatMap(keyMeta => keyMeta.getTable(tableName))
    toOption(tableOption) match {
      case Some(t) => t
      case None =>
        val metadata: Metadata = session.getMetadata
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
        val errorMessage = NameTools.getErrorString(keyspaceName, Some(tableName), suggestions)
        throw new IOException(errorMessage)
    }
  }
}
