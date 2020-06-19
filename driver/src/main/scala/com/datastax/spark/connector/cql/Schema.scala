package com.datastax.spark.connector.cql

import java.io.IOException

import com.datastax.oss.driver.api.core.{CqlSession, ProtocolVersion}
import com.datastax.oss.driver.api.core.metadata.Metadata
import com.datastax.oss.driver.api.core.metadata.schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.types.{ColumnType, CounterType, UserDefinedType}
import com.datastax.spark.connector.util.DriverUtil.toName
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
    columnsToCheck.filterNot(c => columnByName.contains(c.columnName))

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

trait ColumnMetadataAware {
  def columnMetadata:ColumnMetadata
}

trait RelationMetadataAware {
  def relationMetadata:RelationMetadata
}

trait KeyspaceMetadataAware {
  def keyspaceMetadata:KeyspaceMetadata
}

trait IndexMetadataAware {
  def indexMetadata:IndexMetadata
}

trait ColumnDef extends FieldDef {

  def columnName:String

  def columnType:ColumnType[_]

  def ref: ColumnRef

  def isStatic: Boolean

  def isCollection: Boolean

  def isFrozen: Boolean

  def isMultiCell: Boolean

  def isPartitionKeyColumn: Boolean

  def isClusteringColumn: Boolean

  def isPrimaryKeyColumn: Boolean

  def isCounterColumn: Boolean

  def componentIndex: Option[Int]

  def sortingDirection: Option[ClusteringColumn.SortingOrder]
}

case class DefaultColumnDef(
    columnName: String,
    columnRole: ColumnRole,
    columnType: ColumnType[_])
  extends ColumnDef {

  override def ref: ColumnRef = ColumnName(columnName)

  override def isStatic = columnRole == StaticColumn

  override def isCollection = columnType.isCollection

  override def isFrozen = columnType.isFrozen

  override def isMultiCell = columnType.isMultiCell

  override def isPartitionKeyColumn = columnRole == PartitionKeyColumn

  override def isClusteringColumn = columnRole.isInstanceOf[ClusteringColumn]

  override def isPrimaryKeyColumn = isClusteringColumn || isPartitionKeyColumn

  override def isCounterColumn = columnType == CounterType

  override def componentIndex: Option[Int] = columnRole match {
    case ClusteringColumn(i, _) => Some(i)
    case _ => None
  }

  override def sortingDirection: Option[ClusteringColumn.SortingOrder] = columnRole match {
    case ClusteringColumn(_, v) => Some(v)
    case _ => None
  }

  def cql = {
    s"${quote(columnName)} ${columnType.cqlTypeName}"
  }
}

case class DriverColumnDef(column:ColumnMetadata,
                     relation:RelationMetadata,
                     keyspace:KeyspaceMetadata)
  extends ColumnDef
    with KeyspaceMetadataAware
    with RelationMetadataAware
    with ColumnMetadataAware {

  override def ref: ColumnRef = ColumnName(columnName)

  override def columnName = DriverUtil.toName(column.getName)

  override def columnType: ColumnType[_] = ColumnType.fromDriverType(column.getType)

  override def isStatic = column.isStatic

  override def isCollection = columnType.isCollection

  override def isFrozen = columnType.isFrozen

  override def isMultiCell = columnType.isMultiCell

  override def isPartitionKeyColumn = relation.getPartitionKey.asScala.contains(column)

  override def isClusteringColumn = relation.getClusteringColumns.keySet().asScala.contains(column)

  override def isPrimaryKeyColumn = isClusteringColumn || isPartitionKeyColumn

  override def isCounterColumn = columnType == CounterType

  override def sortingDirection: Option[ClusteringColumn.SortingOrder] =
    relation.getClusteringColumns.asScala.get(column).map {
      _ match {
        case ClusteringOrder.ASC => ClusteringColumn.Ascending
        case ClusteringOrder.DESC => ClusteringColumn.Descending
      }
    }

  /* TODO */
  override def componentIndex = ???

  override def columnMetadata = column

  override def relationMetadata = relation

  override def keyspaceMetadata:KeyspaceMetadata = keyspace
}

object DefaultColumnDef {

  def apply(column: ColumnMetadata,
            columnRole: ColumnRole): ColumnDef = {

    val columnType = ColumnType.fromDriverType(column.getType)
    DefaultColumnDef(toName(column.getName), columnRole, columnType)
  }

  def computeColumnRole(colDef:ColumnDef, clusteringCols:Seq[String]):ColumnRole = {
    if (colDef.isPartitionKeyColumn)
      PartitionKeyColumn
    else if (colDef.isClusteringColumn) {
      val idx = clusteringCols.indexOf(colDef.columnName)
      ClusteringColumn(idx, colDef.sortingDirection.get)
    }
    else if (colDef.isStatic)
      StaticColumn
    else
      RegularColumn
  }

  def fromDriverDef(clusteringCols:Seq[String])(driverDef:DriverColumnDef):DefaultColumnDef =
    DefaultColumnDef(
      driverDef.columnName,
      computeColumnRole(driverDef, clusteringCols),
      driverDef.columnType)
}

trait IndexDef extends Serializable {

  def className: Option[String]

  def target: String

  def indexName: String

  def options: Map[String, String]
}

/**
  * @param className If this index is custom, the name of the server-side implementation. Otherwise, empty.
  */
case class DefaultIndexDef(
    className: Option[String],
    target: String,
    indexName: String,
    options: Map[String, String])
  extends IndexDef

case class DriverIndexDef(index:IndexMetadata,
                         relation:RelationMetadata,
                         keyspace:KeyspaceMetadata)
  extends IndexDef
    with KeyspaceMetadataAware
    with RelationMetadataAware
    with IndexMetadataAware {

  override def className = DriverUtil.toOption(index.getClassName)

  override def target = index.getTarget

  override def indexName = DriverUtil.toName(index.getName)

  override def options = index.getOptions.asScala.toMap

  override def indexMetadata = index

  override def relationMetadata = relation

  override def keyspaceMetadata:KeyspaceMetadata = keyspace
}

object DefaultIndexDef {
  def fromDriverDef(driverDef:DriverIndexDef):DefaultIndexDef =
    DefaultIndexDef(
      driverDef.className,
      driverDef.target,
      driverDef.indexName,
      driverDef.options)
}

trait TableDef extends StructDef {

  type ValueRepr = CassandraRow
  type Column <: ColumnDef

  val keyspaceName:String
  val tableName:String
  val partitionKey: Seq[ColumnDef]
  val clusteringColumns: Seq[ColumnDef]
  val regularColumns: Seq[ColumnDef]
  val indexes: Seq[IndexDef]
  val isView:Boolean
  val options:Map[String,String]

  val indexedColumns: Seq[ColumnDef]
  val primaryKey: IndexedSeq[ColumnDef]

  def isIndexed(column: String): Boolean

  def isIndexed(column: ColumnDef): Boolean

  def columnByNameIgnoreCase(columnName: String):ColumnDef
}

case class DefaultTableDef(keyspaceName: String,
                           tableName: String,
                           partitionKey: Seq[DefaultColumnDef],
                           clusteringColumns: Seq[DefaultColumnDef],
                           regularColumns: Seq[DefaultColumnDef],
                           indexes: Seq[DefaultIndexDef] = Seq.empty,
                           isView: Boolean = false,
                           options: Map[String, String] = Map(),
                           ifNotExists: Boolean = false)
  extends TableDef {

  require(partitionKey.forall(_.isPartitionKeyColumn), "All partition key columns must have role PartitionKeyColumn")
  require(clusteringColumns.forall(_.isClusteringColumn), "All clustering columns must have role ClusteringColumn")
  require(regularColumns.forall(!_.isPrimaryKeyColumn), "Regular columns cannot have role PrimaryKeyColumn")

  override type Column = DefaultColumnDef

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

  override val name: String = s"$keyspaceName.$tableName"

  override lazy val primaryKey: IndexedSeq[DefaultColumnDef] =
    (partitionKey ++ clusteringColumns).toIndexedSeq

  override val columns: IndexedSeq[Column] =
    (primaryKey ++ regularColumns).toIndexedSeq

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
    val clusterOrder =
      if (clusteringColumns.isEmpty ||
        clusteringColumns.forall(
          x => x.sortingDirection.getOrElse(() => "") == ClusteringColumn.Ascending))
        Seq()
      else
        Seq("CLUSTERING ORDER BY (" + clusteringColumns.map(x=> quote(x.columnName) +
          " " + x.sortingDirection.getOrElse(() => "")).mkString(", ") + ")")

    val allOptions = clusterOrder ++ options.map(x => x._1 + " = " + x._2)
    val allOptionsStr = if (allOptions.isEmpty)
      ""
    else
      allOptions.mkString(" WITH ", "\n  AND ", "")

    s"""CREATE TABLE $addIfNotExists${quote(keyspaceName)}.${quote(tableName)} (
       |  $columnList,
       |  PRIMARY KEY ($primaryKeyClause)
       |)$allOptionsStr""".stripMargin
  }

  lazy val rowMetadata = CassandraRowMetadata.fromColumnNames(columnNames)

  def newInstance(columnValues: Any*): CassandraRow = {
    new CassandraRow(rowMetadata, columnValues.asInstanceOf[IndexedSeq[AnyRef]])
  }
}

case class DriverTableDef(relation:RelationMetadata,
                          keyspace:KeyspaceMetadata)
  extends TableDef
    with KeyspaceMetadataAware
    with RelationMetadataAware {

  override type Column = DriverColumnDef

  override val keyspaceName = DriverUtil.toName(keyspace.getName)
  override val tableName = DriverUtil.toName(relation.getName)
  override val columns: IndexedSeq[Column] =
    relation.getColumns.asScala.values.map(DriverColumnDef(_, relation, keyspace)).toIndexedSeq
  override val partitionKey: Seq[DriverColumnDef] = {
    val partitionKeyNames =
      relation.getPartitionKey.asScala.map(c => DriverUtil.toName(c.getName)).toSet
    columns.filter(c => partitionKeyNames(c.columnName))
  }
  override val clusteringColumns: Seq[DriverColumnDef] = {
    val clusteringColumnNames =
      relation.getClusteringColumns.keySet.asScala.map(c => DriverUtil.toName(c.getName)).toSet
    columns.filter(c => clusteringColumnNames(c.columnName))
  }
  override lazy val primaryKey: IndexedSeq[DriverColumnDef] =
    (partitionKey ++ clusteringColumns).toIndexedSeq
  override val regularColumns: Seq[DriverColumnDef] = columns.filterNot(primaryKey.toSet)

  val isTable = relation.isInstanceOf[TableMetadata]
  override val isView = relation.isInstanceOf[ViewMetadata]
  val options:Map[String,String] = relation.getOptions.asScala.toMap.map {
    (kv) => (DriverUtil.toName(kv._1), kv._2.toString)
  }

  val asTable = if (isTable) Some(relation.asInstanceOf[TableMetadata]) else Option.empty
  val asView = if (isView) Some(relation.asInstanceOf[ViewMetadata]) else Option.empty

  val indexes:Seq[DriverIndexDef] =
    asTable match {
      case Some(table: TableMetadata) =>
        table.getIndexes.values().asScala.map(i => DriverIndexDef(i, table, keyspace)).toSeq
      case None => Seq.empty
    }

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

  override val name: String = s"${keyspace.getName}.${relation.getName}"

  private lazy val columnBylowerCaseName: Map[String, ColumnDef] = columnByName.map(e => (e._1.toLowerCase, e._2))

  def columnByNameIgnoreCase(columnName: String) = {
    columnBylowerCaseName(columnName.toLowerCase)
  }

  def cql = relation.describe(true)

  lazy val rowMetadata = CassandraRowMetadata.fromColumnNames(columnNames)

  def newInstance(columnValues: Any*): CassandraRow = {
    new CassandraRow(rowMetadata, columnValues.asInstanceOf[IndexedSeq[AnyRef]])
  }

  override def relationMetadata = relation

  override def keyspaceMetadata:KeyspaceMetadata = keyspace
}

object DefaultTableDef {

  /** Constructs a table definition based on the mapping provided by
   * appropriate [[com.datastax.spark.connector.mapper.ColumnMapper]] for the given type. */
  def fromType[T: ColumnMapper](
                                 keyspaceName: String,
                                 tableName: String,
                                 protocolVersion: ProtocolVersion = ProtocolVersion.DEFAULT): DefaultTableDef =
    implicitly[ColumnMapper[T]].newTable(keyspaceName, tableName, protocolVersion)

  def fromDriverDef(driverDef: DriverTableDef): DefaultTableDef = {

    val mapFn = DefaultColumnDef.fromDriverDef(driverDef.clusteringColumns.map(_.columnName))(_)
    DefaultTableDef(
      driverDef.keyspaceName,
      driverDef.tableName,
      driverDef.partitionKey.map(mapFn),
      driverDef.clusteringColumns.map(mapFn),
      driverDef.regularColumns.map(mapFn),
      driverDef.indexes.map(DefaultIndexDef.fromDriverDef(_)),
      driverDef.isView,
      driverDef.options)
  }
}

/* KeyspaceDef is only created at schema load time so we don't bother with the trait + default + driver distinction */
case class KeyspaceDef(keyspaceMetadata: KeyspaceMetadata) extends KeyspaceMetadataAware {

  def keyspaceName:String = DriverUtil.toName(keyspaceMetadata.getName)

  lazy val tableByName: Map[String, TableDef] =
    keyspaceMetadata.getTables.asScala
      .map(idAndTable => (DriverUtil.toName(idAndTable._1), DriverTableDef(idAndTable._2, keyspaceMetadata)))
      .toMap

  lazy val userTypeByName: Map[String, UserDefinedType] =
    keyspaceMetadata.getUserDefinedTypes.asScala
      .map(idAndUdt => (DriverUtil.toName(idAndUdt._1), UserDefinedType(idAndUdt._2)))
      .toMap

  lazy val tablesAndViews: Seq[RelationMetadata] =
    keyspaceMetadata.getTables.asScala.values.toSeq ++ keyspaceMetadata.getViews.asScala.values.toSeq
}

case class Schema(keyspaces: Set[KeyspaceDef]) {

  /** Returns a map from keyspace name to keyspace metadata */
  lazy val keyspaceByName: Map[String, KeyspaceDef] =
    keyspaces.map(keyspaceDef => (keyspaceDef.keyspaceName, keyspaceDef)).toMap

  /** All tables from all keyspaces */
  lazy val tables: Set[TableDef] =
    keyspaces.flatMap(keyspaceDef =>
      keyspaceDef.tablesAndViews
        .map(relation => DriverTableDef(relation,keyspaceDef.keyspaceMetadata)))
}

object Schema extends StrictLogging {

  /** Fetches database schema from Cassandra. Provides access to keyspace, table and column metadata.
   *
   * @param keyspaceName if defined, fetches only metadata of the given keyspace
   * @param tableName    if defined, fetches only metadata of the given table
   */
  def fromCassandra(session: CqlSession,
                    keyspaceName: Option[String] = None,
                    tableName: Option[String] = None): Schema = {

    def isKeyspaceSelected(keyspace: KeyspaceMetadata): Boolean =
      keyspaceName match {
        case None => true
        case Some(name) => toName(keyspace.getName) == name
      }

    def fetchKeyspaces(metadata: Metadata): Set[KeyspaceDef] =
      metadata.getKeyspaces.values().asScala
        .filter(isKeyspaceSelected(_))
        .map(KeyspaceDef(_))
        .toSet

    logger.debug(s"Retrieving database schema")
    val schema = Schema(fetchKeyspaces(session.refreshSchema()))
    logger.debug(s"${schema.keyspaces.size} keyspaces fetched: " +
      s"${schema.keyspaces.map(_.keyspaceName).mkString("{", ",", "}")}")
    schema
  }

  /**
   * Fetches a TableDef for a particular Cassandra Table throws an
   * exception with name options if the table is not found.
   */
  def tableFromCassandra(session: CqlSession,
                         keyspaceName: String,
                         tableName: String): DriverTableDef = {

    fromCassandra(session, Some(keyspaceName), Some(tableName)).tables.headOption match {
      case Some(t:DriverTableDef) => t
      case None =>
        val metadata: Metadata = session.getMetadata
        val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
        val errorMessage = NameTools.getErrorString(keyspaceName, Some(tableName), suggestions)
        throw new IOException(errorMessage)
    }
  }
}
