package org.apache.spark.sql.cassandra

import scala.reflect.ClassTag

import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{types, DataFrame, Row, SQLContext}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef}
import com.datastax.spark.connector.rdd.{ReadConf, ValidRDDType}
import com.datastax.spark.connector.writer.{WriteConf, SqlRowWriter}

import DataTypeConverter._

/**
 *  Implements [[BaseRelation]] and [[InsertableRelation]].
 *  It inserts data into Cassandra table. It also provides some
 *  helper methods and variables for sub class to use.
 */
private[cassandra] abstract class BaseRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with Serializable
  with Logging {

  protected[this] val tableDef =
    sqlContext.getCassandraSchema(tableIdent.cluster.getOrElse("default"))
      .keyspaceByName(tableIdent.keyspace).tableByName(tableIdent.table)

  override def schema: StructType = {
    userSpecifiedSchema.getOrElse(StructType(tableDef.allColumns.map(toStructField)))
  }

  protected[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](
      tableIdent.keyspace,
      tableIdent.table)(
        connector,
        readConf,
        implicitly[ClassTag[CassandraSQLRow]],
        CassandraSQLRowReader,
        implicitly[ValidRDDType[CassandraSQLRow]])

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      val clusterName = tableIdent.cluster.getOrElse(sqlContext.getCluster)
      new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
        .withSessionDo {
        session => session.execute(s"TRUNCATE ${quoted(tableIdent.keyspace)}.${quoted(tableIdent.table)}")
      }
    }

    data.rdd.saveToCassandra(
      tableIdent.keyspace,
      tableIdent.table,
      AllColumns,
      writeConf)(
        connector,
        SqlRowWriter.Factory)
  }

  override def sizeInBytes: Long = {
    val keyspace = tableIdent.keyspace
    val table = tableIdent.table
    val size = sqlContext.conf.getConf(s"spark.cassandra.$keyspace.$table.size.in.bytes", null)
    if (size != null) {
      size.toLong
    } else {
      sqlContext.conf.defaultSizeInBytes
    }
  }

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  /** Quote name */
  private def quoted(str: String): String = {
    "\"" + str + "\""
  }
}

/**
 * Implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedScan]]
 * It inserts data to and scans Cassandra table. It passes the required columns to connector
 * so that only those columns return.
 */
private[cassandra] class PrunedScanRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    tableIdent,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with PrunedScan {

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val transformer = new RddTransformer(requiredColumns, Seq.empty)
    transformer.maybeSelect(baseRdd)
  }
}

/**
 * Implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedFilteredScan]]
 * It inserts data to and scans Cassandra table. Some filters are pushed down to connector,
 * others are combined into a combination filter which is applied to the data returned from connector.
 * In case there's some issue, switch to [[PrunedScan]] which doesn't push down any filters.
 *
 * This is the default scanner to access Cassandra.
 */
private[cassandra] class PrunedFilteredScanRelationImpl(
    tableIdent: TableIdent,
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    tableIdent,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with PrunedFilteredScan {

  /** Map column to data type */
  private val columnDataTypes: Map[String, DataType] =
    tableDef.allColumns.map(columnDef => (columnDef.columnName, columnDataType(columnDef))).toMap

  /** Return a catalyst data type for the column */
  private def columnDataType(columnDef: ColumnDef) : types.DataType = {
    catalystDataType(tableDef.columnByName(columnDef.columnName).columnType, nullable = true)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    logInfo(s"filters: ${filters.mkString(", ")}")

    val pushDown = new PushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"other filters: ${preservedFilters.toString()}")
    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    /** Map column to index */
    val columnIndexes: Map[String, Int] =
      requiredColumns.map(column => (column, requiredColumns.indexOf(column))).toMap

    /**
     * Compare column value and data value, throw exception if there is null.
     * It's used for Larger than, Less than, Larger than or Equal to and Less than or Equal to comparison.
     */
    def compare(columnValue: Any, columnDataType: NativeType, value: Any) : Int = {
      if (columnValue == null) {
          throw new RuntimeException(s"Can't compare column having null value")
      } else {
        columnDataType.ordering.compare(
          columnValue.asInstanceOf[columnDataType.JvmType],
          value.asInstanceOf[columnDataType.JvmType])
      }
    }

    /** Equal comparison of column value and data value. */
    def equalTo(columnValue: Any, columnDataType: NativeType, value: Any) : Boolean = {
      if (columnValue == null) {
          // value couldn't be null. If value is null, query planner doesn't
          // push the filter to data source
          false
      } else {
        // No type check, Catalyst only pushes down filters with the correct value data type
        columnDataType.ordering.compare(
          columnValue.asInstanceOf[columnDataType.JvmType],
          value.asInstanceOf[columnDataType.JvmType]) == 0
      }
    }

    /** Custom Filter stores column index and column data type for Comparing filters */
    case class RichEqualTo(colIndex: Int, colDataType: NativeType, value: Any) extends Filter
    case class RichGreaterThan(colIndex: Int, colDataType: NativeType, value: Any) extends Filter
    case class RichGreaterThanOrEqual(colIndex: Int, colDataType: NativeType, value: Any) extends Filter
    case class RichLessThan(colIndex: Int, colDataType: NativeType, value: Any) extends Filter
    case class RichLessThanOrEqual(colIndex: Int, colDataType: NativeType, value: Any) extends Filter
    case class RichIn(colIndex: Int, values: Array[Any]) extends Filter
    case class RichIsNull(colIndex: Int) extends Filter
    case class RichIsNotNull(colIndex: Int) extends Filter

    /** Return column index in the row and data type */
    def indexAndType(colName: String): (Int, NativeType) = {
      require(colName != null, "Column name can't be null")
      (columnIndexes(colName), columnDataTypes(colName).asInstanceOf[NativeType])
    }

    /**
     *  convert a filter to a RichFilter which stores column index, column data type and
     *  value data type to comparing filter. It is only calculated once for a filter and
     *  the data is used many times for rows filtering.
     */
    def richFilter(filter: Filter) : Filter = filter match {
      case EqualTo(name, v) =>
        val (index, colDataType) = indexAndType(name)
        RichEqualTo(index, colDataType, v)
      case LessThan(name, v) =>
        val (index, colDataType) = indexAndType(name)
        RichLessThan(index, colDataType, v)
      case LessThanOrEqual(name, v) =>
        val (index, colDataType) = indexAndType(name)
        RichLessThanOrEqual(index, colDataType, v)
      case GreaterThan(name, v) =>
        val (index, colDataType) = indexAndType(name)
        RichGreaterThan(index, colDataType, v)
      case GreaterThanOrEqual(name, v) =>
        val (index, colDataType) = indexAndType(name)
        RichGreaterThanOrEqual(index, colDataType, v)
      case IsNotNull(name) =>
        val (index, colDataType) = indexAndType(name)
        RichIsNotNull(index)
      case IsNull(name) =>
        val (index, colDataType) = indexAndType(name)
        RichIsNull(index)
      case In(name, v) =>
        val (index, colDataType) = indexAndType(name)
        RichIn(index, v)
      case And(l, r) => And(richFilter(l), richFilter(r))
      case Or(l, r) => Or(richFilter(l), richFilter(r))
      case Not(f) => Not(richFilter(f))
      case _ => throw new IllegalArgumentException(s"Unknown filter $filter")
    }

    /** Evaluate rich filter on a column value from a row */
    def translateFilter(filter: Filter): Row => Boolean = filter match {
      case RichEqualTo(colIndex, colDataType,  value) =>
        (row: Row) => equalTo(row.get(colIndex), colDataType, value)
      case RichLessThan(colIndex, colDataType, value) =>
        (row: Row) => compare(row.get(colIndex), colDataType, value) < 0
      case RichLessThanOrEqual(colIndex, colDataType, value) =>
        (row: Row) => compare(row.get(colIndex), colDataType, value) <= 0
      case RichGreaterThan(colIndex, colDataType, value) =>
        (row: Row) => compare(row.get(colIndex), colDataType, value) > 0
      case RichGreaterThanOrEqual(colIndex, colDataType, value) =>
        (row: Row) => compare(row.get(colIndex), colDataType, value) >= 0
      case RichIn(colIndex, values) => (row: Row) => values.toSet.contains(row.get(colIndex))
      case RichIsNull(colIndex) => (row: Row) => row.get(colIndex) == null
      case RichIsNotNull(colIndex) => (row: Row) => row.get(colIndex) != null
      case Not(f) => (row: Row) => !translateFilter(f)(row)
      case And(l, r) => (row: Row) => translateFilter(l)(row) && translateFilter(r)(row)
      case Or(l, r) => (row: Row) => translateFilter(l)(row) || translateFilter(r)(row)
      case _ => (row: Row) => throw new RuntimeException(s"Unknown RichFilter $filter")
    }

    // a filter combining all other filters
    val richFilters = preservedFilters.map(richFilter)
    val translatedFilters = richFilters.map(translateFilter)
    def rowFilter(row: Row): Boolean = translatedFilters.forall(_(row))

    val transformer = new RddTransformer(requiredColumns, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }
}

object CassandraSourceRelation {

  /** If push down is disable, use [[PrunedScan]]. By default use [[PrunedFilteredScan]]*/
  def apply(tableIdent: TableIdent, sqlContext: SQLContext)(
    implicit
      connector: CassandraConnector =
      new CassandraConnector(sqlContext.getCassandraConnConf(sqlContext.getCluster)),
      readConf: ReadConf = sqlContext.getReadConf(tableIdent),
      writeConf: WriteConf = sqlContext.getWriteConf(tableIdent),
      sourceOptions: CassandraDataSourceOptions = CassandraDataSourceOptions()) : BaseRelationImpl = {

    // Default scan type
    if (sourceOptions.pushdown) {
      new PrunedFilteredScanRelationImpl(
        tableIdent = tableIdent,
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        userSpecifiedSchema = sourceOptions.schema,
        sqlContext = sqlContext)
    } else {
      new PrunedScanRelationImpl(
        tableIdent = tableIdent,
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        userSpecifiedSchema = sourceOptions.schema,
        sqlContext = sqlContext)
    }
  }
}
