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
    tableSizeInBytes: Long,
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
    userSpecifiedSchema.getOrElse(StructType(tableDef.allColumns.map(toStructField)))
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

  override def sizeInBytes: Long = tableSizeInBytes

  implicit val cassandraConnector = connector
  implicit val readconf = readConf
  private[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](tableRef.keyspace, tableRef.table)

  def buildScan() : RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(s"filters: ${filters.mkString(", ")}")
    val pushdownFilters = if(filterPushdown) new PushDown(filters, tableDef).toPushDown else Seq.empty
    if(filterPushdown) {
      logInfo(s"pushdown filters: ${pushdownFilters.toString()}")
    }

    val transformer = new RddTransformer(requiredColumns, pushdownFilters)
    transformer.transform(baseRdd).asInstanceOf[RDD[Row]]
  }

  /** Add column selection and where clauses on top of baseRdd */
  private[this] class RddTransformer(requiredColumns: Array[String], filters: Seq[Filter]) {

    /** CassandraRDD[CassandraSQLRow] is the only type supported for transferring */
    private type RDDType = CassandraRDD[CassandraSQLRow]

    /** Transfer selection to limit to columns specified */
    def maybeSelect(rdd: RDDType) : RDDType = {
      if (requiredColumns.nonEmpty) {
        rdd.select(requiredColumns.map(column => column: NamedColumnRef): _*)
      } else {
        rdd
      }
    }

    /** Push down filters to CQL query */
    private def maybePushdownFilters(rdd: RDDType) : RDDType = {
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

    /** Transform rdd by applying selected columns and push downed filters */
    def transform(rdd: RDDType) : RDDType = {
      if(filters.isEmpty) {
        maybeSelect(rdd)
      } else {
        maybePushdownFilters(maybeSelect(rdd))
      }
    }
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
      sourceOptions: CassandraSourceOptions = CassandraSourceOptions(),
      schema : Option[StructType] = None) : CassandraSourceRelation = {

    //TODO
    /** Retrieve table size from C* system table. If it's not found, use SQLConf default setting */
    def tableSize = {
      None.getOrElse(sqlContext.conf.defaultSizeInBytes)
    }

    val tableSizeInBytes = sourceOptions.tableSizeInBytes.getOrElse(tableSize)
    val defaultCassandraConnectorConf = CassandraConnectorConf(sqlContext.sparkContext.conf)
    val cassandraConnConf = sourceOptions.cassandraConConf.getOrElse(defaultCassandraConnectorConf)
    val cassandraConnector = new CassandraConnector(cassandraConnConf)
    val defaultReadConf = ReadConf.fromSparkConf(sqlContext.sparkContext.conf)
    val readConf = sourceOptions.readConf.getOrElse(defaultReadConf)
    val defaultWriteConf= WriteConf.fromSparkConf(sqlContext.sparkContext.conf)
    val writeConf = sourceOptions.writeConf.getOrElse(defaultWriteConf)

    new CassandraSourceRelation(
      tableRef = tableRef,
      userSpecifiedSchema = schema,
      filterPushdown = sourceOptions.pushdown,
      tableSizeInBytes = tableSizeInBytes,
      connector = cassandraConnector,
      readConf = readConf,
      writeConf = writeConf,
      sqlContext = sqlContext)
  }
}
