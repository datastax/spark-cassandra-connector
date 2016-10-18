package org.apache.spark.sql.cassandra

import java.net.InetAddress
import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.cassandra.DataTypeConverter._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, sources}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.SparkConf

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, Schema}
import com.datastax.spark.connector.rdd.partitioner.CassandraPartitionGenerator._
import com.datastax.spark.connector.rdd.partitioner.DataSizeEstimates
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.forSystemLocalPartitioner
import com.datastax.spark.connector.rdd.{CassandraRDD, ReadConf}
import com.datastax.spark.connector.types.{InetType, UUIDType, VarIntType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util.{ConfigParameter, Logging, ReflectionUtil}
import com.datastax.spark.connector.writer.{SqlRowWriter, WriteConf}
import com.datastax.spark.connector.{SomeColumns, _}

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

  private[this] val tableDef = Schema.tableFromCassandra(
    connector,
    tableRef.keyspace,
    tableRef.table)

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
    val columns = SomeColumns(data.columns.map(x => x: ColumnRef): _*)
    data.rdd.saveToCassandra(tableRef.keyspace, tableRef.table, columns, writeConf)
  }

  override def sizeInBytes: Long = {
    // If it's not found, use SQLConf default setting
    tableSizeInBytes.getOrElse(sqlContext.conf.defaultSizeInBytes)
  }

  implicit val cassandraConnector = connector
  implicit val readconf = readConf
  private[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](tableRef.keyspace, tableRef.table)

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filterPushdown match {
    case true => predicatePushDown(filters).handledBySpark.toArray
    case false => filters
  }

  lazy val additionalRules: Seq[CassandraPredicateRules] = {
    import CassandraSourceRelation.AdditionalCassandraPushDownRulesParam
    val sc = sqlContext.sparkContext

    /* So we can set this in testing to different values without
     making a new context check local property as well */
    val userClasses: Option[String] =
      sc.getConf.getOption(AdditionalCassandraPushDownRulesParam.name)
        .orElse(Option(sc.getLocalProperty(AdditionalCassandraPushDownRulesParam.name)))

    userClasses match {
      case Some(classes) =>
        classes
          .trim
          .split("""\s*,\s*""")
          .map(ReflectionUtil.findGlobalObject[CassandraPredicateRules])
          .reverse
      case None => AdditionalCassandraPushDownRulesParam.default
    }
  }

  private def predicatePushDown(filters: Array[Filter]) = {
    logInfo(s"Input Predicates: [${filters.mkString(", ")}]")

    val pv = connector.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersion)

    /** Apply built in rules **/
    val bcpp = new BasicCassandraPredicatePushDown(filters.toSet, tableDef, pv)
    val basicPushdown = AnalyzedPredicates(bcpp.predicatesToPushDown, bcpp.predicatesToPreserve)
    logDebug(s"Basic Rules Applied:\n$basicPushdown")

    /** Apply any user defined rules **/
    val finalPushdown =  additionalRules.foldRight(basicPushdown)(
      (rules, pushdowns) => {
        val pd = rules(pushdowns, tableDef)
        logDebug(s"Applied ${rules.getClass.getSimpleName} Pushdown Filters:\n$pd")
        pd
      }
    )

    logDebug(s"Final Pushdown filters:\n$finalPushdown")
    finalPushdown
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val prunedRdd = maybeSelect(baseRdd, requiredColumns)
    val prunedFilteredRdd = {
      if(filterPushdown) {
        val pushdownFilters = predicatePushDown(filters).handledByCassandra.toArray
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
        |retrieve size from Cassandra. Can be set manually now""".stripMargin
  )

  val AdditionalCassandraPushDownRulesParam = ConfigParameter[List[CassandraPredicateRules]] (
    name = "spark.cassandra.sql.pushdown.additionalClasses",
    section = ReferenceSection,
    default = List.empty,
    description =
      """A comma separated list of classes to be used (in order) to apply additional
        | pushdown rules for Cassandra Dataframes. Classes must implement CassandraPredicateRules
      """.stripMargin
  )

  val Properties = Seq(
    AdditionalCassandraPushDownRulesParam,
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
        val tokenFactory = forSystemLocalPartitioner(cassandraConnector)
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
    val cluster = tableRef.cluster.getOrElse(defaultClusterName)
    val ks = tableRef.keyspace
    //Keyspace/Cluster level settings
    for (prop <- DefaultSource.confProperties) {
      val value = Seq(
        tableConf.get(prop),
        sqlConf.get(s"$cluster:$ks/$prop"),
        sqlConf.get(s"$cluster/$prop"),
        sqlConf.get(s"default/$prop")).flatten.headOption
      value.foreach(conf.set(prop, _))
    }
    conf
  }
}
