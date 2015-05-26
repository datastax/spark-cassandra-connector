package org.apache.spark.sql.cassandra

import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{sources, DataFrame, Row, SQLContext}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector, Schema, ColumnDef}
import com.datastax.spark.connector.rdd.{CassandraRDD, ReadConf}
import com.datastax.spark.connector.writer.{WriteConf, SqlRowWriter}
import com.datastax.spark.connector.util.Quote._

import DataTypeConverter._

/**
 * Implements [[BaseRelation]]]], [[InsertableRelation]]]] and [[PrunedFilteredScan]]]]
 * It inserts data to and scans Cassandra table. If filterPushdown is true, it pushs down
 * some filters to CQL
 *
 */
private[cassandra] class CassandraSourceRelation(
    tableRef: TableRef,
    userSpecifiedSchema: Option[StructType],
    filterPushdown: Boolean,
    tableSizeInBytes: Option[Long],
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    override val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with PrunedFilteredScan
  with Logging {

  private[this] val tableDef = Schema.fromCassandra(connector)
    .keyspaceByName(tableRef.keyspace).tableByName(tableRef.table)

  override def schema: StructType = {
    userSpecifiedSchema.getOrElse(StructType(tableDef.columns.map(toStructField)))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      connector.withSessionDo {
        val keyspace = quote(tableRef.keyspace)
        val table = quote(tableRef.table)
        session => session.execute(s"TRUNCATE $keyspace.$table")
      }
    }

    implicit val rwf = SqlRowWriter.Factory
    data.rdd.saveToCassandra(tableRef.keyspace, tableRef.table, AllColumns, writeConf)
  }

  override def sizeInBytes: Long = {
    //TODO  Retrieve table size from C* system table from Cassandra 2.1.4
    // If it's not found, use SQLConf default setting
    tableSizeInBytes.getOrElse(sqlContext.conf.defaultSizeInBytes)
  }

  implicit val cassandraConnector = connector
  implicit val readconf = readConf
  private[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](tableRef.keyspace, tableRef.table)

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
      case sources.EqualTo(attribute, value)            => (s"${quote(attribute)} = ?", Seq(value))
      case sources.LessThan(attribute, value)           => (s"${quote(attribute)} < ?", Seq(value))
      case sources.LessThanOrEqual(attribute, value)    => (s"${quote(attribute)} <= ?", Seq(value))
      case sources.GreaterThan(attribute, value)        => (s"${quote(attribute)} > ?", Seq(value))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(value))
      case sources.In(attribute, values)                 =>
        (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), values.toSeq)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
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

  val tableSizeInBytesProperty = "spark.cassandra.table.size.in.bytes"

  val Properties = Seq(
    tableSizeInBytesProperty
  )

  def apply(
      tableRef: TableRef,
      sqlContext: SQLContext,
      options: CassandraSourceOptions = CassandraSourceOptions(),
      schema : Option[StructType] = None) : CassandraSourceRelation = {

    val conf = sqlContext.sparkContext.getConf.clone()
    for (prop <- DefaultSource.confProperties) {
      val tableLevelValue = options.cassandraConfs.get(prop)
      if (tableLevelValue.nonEmpty)
        conf.set(prop, tableLevelValue.get)
    }

    val tableSizeInBytesString = conf.getOption(tableSizeInBytesProperty)
    val tableSizeInBytes = if (tableSizeInBytesString.nonEmpty) Option(tableSizeInBytesString.get.toLong) else None
    val cassandraConnector = new CassandraConnector(CassandraConnectorConf(conf))
    val readConf = ReadConf.fromSparkConf(conf)
    val writeConf = WriteConf.fromSparkConf(conf)

    new CassandraSourceRelation(
      tableRef = tableRef,
      userSpecifiedSchema = schema,
      filterPushdown = options.pushdown,
      tableSizeInBytes = tableSizeInBytes,
      connector = cassandraConnector,
      readConf = readConf,
      writeConf = writeConf,
      sqlContext = sqlContext)
  }
}
