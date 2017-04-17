package org.apache.spark.sql.cassandra

import java.io.IOException
import java.net.InetAddress
import java.util.UUID

import com.datastax.driver.core.Metadata
import com.datastax.spark.connector.rdd.partitioner.DataSizeEstimates
import com.datastax.spark.connector.types.{UUIDType, InetType, VarIntType}
import com.datastax.spark.connector.util.{ConfigParameter, NameTools}
import org.apache.spark.{SparkConf, Logging}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{sources, DataFrame, Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{TableDef, CassandraConnectorConf, CassandraConnector, Schema}
import com.datastax.spark.connector.rdd.{CassandraRDD, ReadConf}
import com.datastax.spark.connector.writer.{WriteConf, SqlRowWriter}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.SomeColumns

import DataTypeConverter._
import com.datastax.spark.connector.rdd.partitioner.CassandraRDDPartitioner._

/**
 * Implements [[BaseRelation]]]], [[InsertableRelation]]]] and [[PrunedFilteredScan]]]]
 * It inserts data to and scans Cassandra table. If filterPushdown is true, it pushs down
 * some filters to CQL
 *
 */
private[cassandra] class CassandraSourceRelation(
    tableDef: TableDef,
    userSpecifiedSchema: Option[StructType],
    filterPushdown: Boolean,
    tableSizeInBytes: Option[Long],
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    preappendColumns: Set[String],
    override val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with PrunedFilteredScan
  with Logging {

  override def schema: StructType = {
    userSpecifiedSchema.getOrElse(StructType(tableDef.columns.map(toStructField)))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      connector.withSessionDo {
        val keyspace = quote(tableDef.keyspaceName)
        val table = quote(tableDef.tableName)
        session => session.execute(s"TRUNCATE $keyspace.$table")
      }
    }

    implicit val rwf = SqlRowWriter.Factory
    val columns = SomeColumns(data.columns.map(x => columnRef(x, overwrite)): _*)
    data.rdd.saveToCassandra(tableDef.keyspaceName, tableDef.tableName, columns, writeConf)
  }

  private[this] def columnRef(columnName: String, overwrite: Boolean): ColumnRef = {
    if (!overwrite && tableDef.columnByName(columnName).isCollection) {
      if (preappendColumns.contains(columnName)) {
        CollectionColumnName(columnName, alias = None, collectionBehavior = CollectionPrepend)
      } else {
        CollectionColumnName(columnName, alias = None, collectionBehavior = CollectionAppend)
      }
    } else {
      ColumnName(columnName)
    }
  }

  override def sizeInBytes: Long = {
    // If it's not found, use SQLConf default setting
    tableSizeInBytes.getOrElse(sqlContext.conf.defaultSizeInBytes)
  }

  implicit val cassandraConnector = connector
  implicit val readconf = readConf
  private[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](tableDef.keyspaceName, tableDef.tableName)

  def buildScan() : RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val prunedRdd = maybeSelect(baseRdd, requiredColumns)
    logInfo(s"filters: ${filters.mkString(", ")}")
    val prunedFilteredRdd = {
      if(filterPushdown) {
        val filterPushdown = new PredicatePushDown(filters.toSet, tableDef)
        val pushdownFilters = filterPushdown.predicatesToPushDown.toSeq
        logInfo(s"pushdown filters: ${pushdownFilters.toString()}")
        val filteredRdd = maybePushdownFilters(prunedRdd, pushdownFilters)
        filteredRdd.asInstanceOf[RDD[Row]]
      } else {
        prunedRdd
      }
    }
    prunedFilteredRdd.asInstanceOf[RDD[Row]]
  }

  /** Define a type for CassandraRDD[CassandraSQLRow]. It's used by following methods */
  private type RDDType = CassandraRDD[CassandraSQLRow]

  /** Transfer selection to limit to columns specified */
  private def maybeSelect(rdd: RDDType, requiredColumns: Array[String]) : RDDType = {
    if (requiredColumns.nonEmpty) {
      rdd.select(requiredColumns.map(column => column: ColumnRef): _*)
    } else {
      rdd
    }
  }

  /** Push down filters to CQL query */
  private def maybePushdownFilters(rdd: RDDType, filters: Seq[Filter]) : RDDType = {
    whereClause(filters) match {
      case (cql, values) if values.nonEmpty => rdd.where(cql, values: _*)
      case _ => rdd
    }
  }

  /** Construct Cql clause and retrieve the values from filter */
  private def filterToCqlAndValue(filter: Any): (String, Seq[Any]) = {
    filter match {
      case sources.EqualTo(attribute, value)            => (s"${quote(attribute)} = ?", Seq(toCqlValue(attribute, value)))
      case sources.LessThan(attribute, value)           => (s"${quote(attribute)} < ?", Seq(toCqlValue(attribute, value)))
      case sources.LessThanOrEqual(attribute, value)    => (s"${quote(attribute)} <= ?", Seq(toCqlValue(attribute, value)))
      case sources.GreaterThan(attribute, value)        => (s"${quote(attribute)} > ?", Seq(toCqlValue(attribute, value)))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(toCqlValue(attribute, value)))
      case sources.In(attribute, values)                 =>
        (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), toCqlValues(attribute, values))
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }

  private def toCqlValues(columnName: String, values: Array[Any]): Seq[Any] = {
    values.map(toCqlValue(columnName, _)).toSeq
  }

  /** If column is VarInt column, convert data to BigInteger */
  private def toCqlValue(columnName: String, value: Any): Any = {
    value match {
      case decimal: Decimal =>
        val isVarIntColumn = tableDef.columnByName(columnName).columnType == VarIntType
        if (isVarIntColumn) decimal.toJavaBigDecimal.toBigInteger else decimal
      case utf8String: UTF8String =>
        val columnType = tableDef.columnByName(columnName).columnType
        if (columnType == InetType) {
          InetAddress.getByName(utf8String.toString)
        } else if(columnType == UUIDType) {
          UUID.fromString(utf8String.toString)
        } else {
          utf8String
        }
      case other => other
    }
  }

  /** Construct where clause from pushdown filters */
  private def whereClause(pushdownFilters: Seq[Any]): (String, Seq[Any]) = {
    val cqlValue = pushdownFilters.map(filterToCqlAndValue)
    val cql = cqlValue.map(_._1).mkString(" AND ")
    val args = cqlValue.flatMap(_._2)
    (cql, args)
  }
}

object CassandraSourceRelation {
  val ReferenceSection = "Cassandra DataFrame Source Parameters"

  val TableSizeInBytesParam = ConfigParameter[Option[Long]](
    name = "spark.cassandra.table.size.in.bytes",
    section = ReferenceSection,
    default = None,
    description =
      """Used by DataFrames Internally, will be updated in a future release to
        |retrieve size from C*. Can be set manually now""".stripMargin
  )

  val Properties = Seq(
    TableSizeInBytesParam
  )

  val defaultClusterName = "default"

  def apply(
    tableRef: TableRef,
    sqlContext: SQLContext,
    options: CassandraSourceOptions = CassandraSourceOptions(),
    schema : Option[StructType] = None) : CassandraSourceRelation = {

    val sparkConf = sqlContext.sparkContext.getConf
    val sqlConf = sqlContext.getAllConfs
    val conf =
      consolidateConfs(sparkConf, sqlConf, tableRef, options.cassandraConfs)
    val tableSizeInBytesString = conf.getOption(TableSizeInBytesParam.name)
    val cassandraConnector =
      new CassandraConnector(CassandraConnectorConf(conf))
    val tableSizeInBytes = tableSizeInBytesString match {
      case Some(size) => Option(size.toLong)
      case None =>
        val tokenFactory = getTokenFactory(cassandraConnector)
        val dataSizeInBytes =
          new DataSizeEstimates(
            cassandraConnector,
            tableRef.keyspace,
            tableRef.table)(tokenFactory).totalDataSizeInBytes
        if (dataSizeInBytes <= 0L) {
          None
        } else {
          Option(dataSizeInBytes)
        }
    }
    val readConf = ReadConf.fromSparkConf(conf)
    val writeConf = WriteConf.fromSparkConf(conf)
    val tableDef = {
      val tableName = tableRef.table
      val keyspaceName = tableRef.keyspace
      Schema.fromCassandra(cassandraConnector, Some(keyspaceName), Some(tableName)).tables.headOption match {
        case Some(t) => t
        case None =>
          val metadata: Metadata = cassandraConnector.withClusterDo(_.getMetadata)
          val suggestions = NameTools.getSuggestions(metadata, keyspaceName, tableName)
          val errorMessage = NameTools.getErrorString(keyspaceName, tableName, suggestions)
          throw new IOException(errorMessage)
      }
    }
    val preappendColumns = tableDef.columns.map(_.columnName).filter(preappendColumn(_, options.cassandraConfs)).toSet

    new CassandraSourceRelation(
      tableDef = tableDef,
      userSpecifiedSchema = schema,
      filterPushdown = options.pushdown,
      tableSizeInBytes = tableSizeInBytes,
      connector = cassandraConnector,
      readConf = readConf,
      writeConf = writeConf,
      preappendColumns = preappendColumns,
      sqlContext = sqlContext)
  }

  /**
   * Consolidate Cassandra conf settings in the order of
   * table level -> keyspace level -> cluster level ->
   * default. Use the first available setting. Default
   * settings are stored in SparkConf.
   */
  def consolidateConfs(
    sparkConf: SparkConf,
    sqlConf: Map[String, String],
    tableRef: TableRef,
    tableConf: Map[String, String]) : SparkConf = {
    //Default settings
    val conf = sparkConf.clone()
    //Keyspace/Cluster level settings
    for (prop <- DefaultSource.confProperties) {
      val cluster = tableRef.cluster.getOrElse(defaultClusterName)
      val clusterLevelValue = sqlConf.get(s"$cluster/$prop")
      if (clusterLevelValue.nonEmpty)
        conf.set(prop, clusterLevelValue.get)
      val keyspaceLevelValue =
        sqlConf.get(s"$cluster:${tableRef.keyspace}/$prop")
      if (keyspaceLevelValue.nonEmpty)
        conf.set(prop, keyspaceLevelValue.get)
      val tableLevelValue = tableConf.get(prop)
      if (tableLevelValue.nonEmpty)
        conf.set(prop, tableLevelValue.get)
    }
    conf
  }

  def preappendColumn(columnName: String, cassandraConfs: Map[String, String]): Boolean = {
    val preappendSetting = cassandraConfs.get(s"$columnName.preappend")
    preappendSetting.nonEmpty && preappendSetting.get.toLowerCase.equals("true")
  }
}
