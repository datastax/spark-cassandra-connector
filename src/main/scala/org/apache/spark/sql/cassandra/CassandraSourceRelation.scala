package org.apache.spark.sql.cassandra

import java.net.InetAddress
import java.util.{Locale, UUID}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.cassandra.DataTypeConverter._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, Schema}
import com.datastax.spark.connector.rdd.partitioner.DataSizeEstimates
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.forSystemLocalPartitioner
import com.datastax.spark.connector.rdd.{CassandraRDD, CassandraTableScanRDD, ReadConf}
import com.datastax.spark.connector.types.{InetType, UUIDType, VarIntType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer.{SqlRowWriter, WriteConf}
import com.datastax.spark.connector.{SomeColumns, _}

sealed trait DirectJoinSetting
case object AlwaysOn extends DirectJoinSetting
case object AlwaysOff extends DirectJoinSetting
case object Automatic extends DirectJoinSetting

sealed trait DseSearchOptimizationSetting { def enabled = false }
case object On extends DseSearchOptimizationSetting { override def enabled = true }
case object Off extends DseSearchOptimizationSetting
case class Auto(ratio: Double) extends DseSearchOptimizationSetting { override def enabled = true }

import scala.collection.concurrent.TrieMap

/**
 * Implements [[BaseRelation]]]], [[InsertableRelation]]]] and [[PrunedFilteredScan]]]]
 * It inserts data to and scans Cassandra table. If filterPushdown is true, it pushs down
 * some filters to CQL
 *
 */
class CassandraSourceRelation(
    val tableRef: TableRef,
    val userSpecifiedSchema: Option[StructType],
    val filterPushdown: Boolean,
    val confirmTruncate: Boolean,
    val tableSizeInBytes: Option[Long],
    val connector: CassandraConnector,
    val readConf: ReadConf,
    val writeConf: WriteConf,
    val sparkConf: SparkConf,
    override val sqlContext: SQLContext,
    val directJoinSetting: DirectJoinSetting = Automatic)
  extends BaseRelation
  with InsertableRelation
  with PrunedFilteredScan
  with Logging {

  import CassandraSourceRelation._

  def withDirectJoin(directJoinSetting: DirectJoinSetting) = {
    new CassandraSourceRelation(
      tableRef,
      userSpecifiedSchema,
      filterPushdown,
      confirmTruncate,
      tableSizeInBytes,
      connector,
      readConf,
      writeConf,
      sparkConf,
      sqlContext,
      directJoinSetting = directJoinSetting
    )
  }

  val tableDef = Schema.tableFromCassandra(
    connector,
    tableRef.keyspace,
    tableRef.table)

  val searchOptimization: DseSearchOptimizationSetting =
    sparkConf.get(
      CassandraSourceRelation.SearchPredicateOptimizationParam.name,
      CassandraSourceRelation.SearchPredicateOptimizationParam.default
    ).toLowerCase match {
      case "auto" => Auto(sparkConf.getDouble(
          CassandraSourceRelation.SearchPredicateOptimizationRatioParam.name,
          CassandraSourceRelation.SearchPredicateOptimizationRatioParam.default))
      case "on" | "true" => On
      case "off" | "false" => Off
      case unknown => throw new IllegalArgumentException(
        s"""
           |Attempted to set ${CassandraSourceRelation.SearchPredicateOptimizationParam.name} to
           |$unknown which is invalid. Acceptable values are: auto, on, and off
           """.stripMargin)
    }

  override def schema: StructType = {
    userSpecifiedSchema.getOrElse(StructType(tableDef.columns.map(toStructField)))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      if (confirmTruncate) {
        connector.withSessionDo { session =>
          val keyspace = quote(tableRef.keyspace)
          val table = quote(tableRef.table)
          val stmt = maybeExecutingAs(session.prepare(s"TRUNCATE $keyspace.$table").bind(), writeConf.executeAs)
          session.execute(stmt)
        }
      } else {
        throw new UnsupportedOperationException(
          """You are attempting to use overwrite mode which will truncate
          |this table prior to inserting data. If you would merely like
          |to change data already in the table use the "Append" mode.
          |To actually truncate please pass in true value to the option
          |"confirm.truncate" when saving. """.stripMargin)
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

  /*
  Eliminate duplicate calculation of predicate pushdowns
  (once for unhandled filters and once for actually building the scan)
  */
  val pushdownCache: TrieMap[Seq[Filter], AnalyzedPredicates] = TrieMap.empty

  private def predicatePushDown(filters: Array[Filter]) = pushdownCache.getOrElseUpdate(filters.toSeq, {

    logDebug(s"Input Predicates: [${filters.mkString(", ")}]")

    val pv = connector.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersion)

    /** Apply built in rules **/
    val bcpp = new BasicCassandraPredicatePushDown(filters.toSet, tableDef, pv)
    val basicPushdown = AnalyzedPredicates(bcpp.predicatesToPushDown, bcpp.predicatesToPreserve)
    logDebug(s"Basic Rules Applied:\n$basicPushdown")

    logDebug(s"Applying DSE Only Predicate Rules")
    /** Apply Dse Predicate Rules **/
    val dsePredicates: AnalyzedPredicates = {
      val dseBasicPredicates = DsePredicateRules.apply(basicPushdown, tableDef, sparkConf)
      if (searchOptimization.enabled) {
        logDebug(s"Search Optimization Enabled - $searchOptimization")
        SolrPredicateRules.apply(dseBasicPredicates, tableDef, sparkConf, searchOptimization)
      } else {
          dseBasicPredicates
      }
    }
    logDebug(s"Applied DSE Predicate Rules Pushdown Filters: \n$dsePredicates")

    /** Apply any user defined rules **/
    val finalPushdown =  additionalRules.foldRight(dsePredicates)(
      (rules, pushdowns) => {
        val pd = rules(pushdowns, tableDef, sparkConf)
        logDebug(s"Applied ${rules.getClass.getSimpleName} Pushdown Filters:\n$pd")
        pd
      }
    )

    logDebug(s"Final Pushdown filters:\n$finalPushdown")
    finalPushdown
  })

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val filteredRdd = {
      if (filterPushdown) {
        val analyzedPredicates = predicatePushDown(filters)
        logDebug(s"Building RDD with filters:\n$analyzedPredicates")
        val pushdownFilters = analyzedPredicates.handledByCassandra.toArray
        maybePushdownFilters(baseRdd, pushdownFilters)
      } else {
        baseRdd
      }
    }
    maybeSelect(filteredRdd, requiredColumns)
  }

  /** Define a type for CassandraRDD[CassandraSQLRow]. It's used by following methods */
  private type RDDType = CassandraRDD[CassandraSQLRow]

  /** Transfer selection to limit to columns specified */
  private def maybeSelect(rdd: RDDType, requiredColumns: Array[String]) : RDD[Row] = {
    val prunedRdd = if (requiredColumns.nonEmpty) {
      rdd.select(requiredColumns.map(column => column: ColumnRef): _*)
    } else {
      rdd match {
        case rdd: CassandraTableScanRDD[_] =>
          val tableIsSolrIndexed =
            rdd.tableDef
              .indexes
              .exists(index => index.className == SolrConstants.DseSolrIndexClassName)
          val countRDD =
            if (searchOptimization.enabled && tableIsSolrIndexed && rdd.where.predicates.isEmpty){
              //This will shortcut actually reading the rows out of Cassandra and just hit the
              //solr indexes
              CassandraTableScanRDD.countRDD(rdd).where(s"${SolrConstants.SolrQuery} = '*:*'")
            } else {
              CassandraTableScanRDD.countRDD(rdd)
            }
            countRDD.mapPartitions(_.flatMap(count => Iterator.fill(count.toInt)(CassandraSQLRow.empty)))
        case _ => rdd
      }
    }
    prunedRdd.asInstanceOf[RDD[Row]]
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

  def canEqual(a: Any) = a.isInstanceOf[CassandraSourceRelation]

  override def equals(that: Any): Boolean =
    that match {
      case that: CassandraSourceRelation => that.canEqual(this) &&
          this.hashCode == that.hashCode &&
          this.tableRef == that.tableRef &&
          this.confirmTruncate == that.confirmTruncate &&
          this.readconf == that.readconf &&
          this.writeConf == that.writeConf &&
          this.userSpecifiedSchema == that.userSpecifiedSchema &&
          this.filterPushdown == that.filterPushdown &&
          this.tableSizeInBytes == that.tableSizeInBytes &&
          this.cassandraConnector.conf == that.cassandraConnector.conf &&
          this.directJoinSetting == that.directJoinSetting
      case _ => false
    }

  override lazy val hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + tableRef.hashCode
    result = prime * result + confirmTruncate.hashCode
    result = prime * result + readconf.hashCode
    result = prime * result + writeConf.hashCode
    result = prime * result + userSpecifiedSchema.hashCode
    result = prime * result + filterPushdown.hashCode
    result = prime * result + tableSizeInBytes.hashCode
    result = prime * result + this.cassandraConnector.conf.hashCode

    result
  }

  override def toString : String = this.getClass.getCanonicalName
}

object CassandraSourceRelation extends Logging {
  private lazy val hiveConf = new HiveConf()

  val ReferenceSection = "Cassandra Datasource Parameters"
  val DseReferenceSection = "DSE Exclusive Datasource Parameters"

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

  val SearchPredicateOptimizationRatioParam = ConfigParameter[Double] (
    name = "spark.sql.dse.search.autoRatio",
    section = DseReferenceSection,
    default = 0.03,
    description = "When Search Predicate Optimization is set to auto, Search optimizations will be " +
      "preformed if this parameter * the total number of rows is greater than the number of rows" +
      "to be returned by the solr query"
  )

  val SearchPredicateOptimizationParam = ConfigParameter[String] (
    name = "spark.sql.dse.search.enableOptimization",
    section = DseReferenceSection,
    default = "auto",
    description =
      s"""Enables SparkSQL to automatically replace Cassandra Pushdowns with DSE Search
        |Pushdowns utilizing lucene indexes. Valid options are On, Off, and Auto. Auto enables
        |optimizations when the solr query will pull less than $SearchPredicateOptimizationRatioParam * the
        |total table record count""".stripMargin
  )

  val SolrPredciateOptimizationParam = DeprecatedConfigParameter (
    name = "spark.sql.dse.solr.enable_optimization",
    replacementParameter = Some(SearchPredicateOptimizationParam),
    deprecatedSince = "DSE 6.0.0"
  )

  val DirectJoinSizeRatioParam = ConfigParameter[Double] (
    name = "directJoinSizeRatio",
    section = DseReferenceSection,
    default = 0.9d,
    description =
      s"""
         | Sets the threshold on when to perform a DirectJoin in place of a full table scan. When
         | the size of the (CassandraSource * thisParameter) > The other side of the join, A direct
         | join will be performed if possible.
      """.stripMargin
  )

  val DirectJoinSettingParam = ConfigParameter[String] (
    name = "directJoinSetting",
    section = DseReferenceSection,
    default = "auto",
    description =
      s"""Acceptable values, "on", "off", "auto"
        |"on" causes a direct join to happen if possible regardless of size ratio.
        |"off" disables direct join even when possible
        |"auto" only does a direct join when the size ratio is satisfied see ${DirectJoinSizeRatioParam.name}
      """.stripMargin
  )

  val defaultClusterName = "default"

  private val proxyPerSourceRelationEnabled = sys.env.getOrElse("DSE_ENABLE_PROXY_PER_SRC_RELATION", "false").toBoolean

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

    lazy val proxyUser = if (proxyPerSourceRelationEnabled) getProxyUser(sqlContext) else None
    val readConf = {
      val rc = ReadConf.fromSparkConf(conf)
      rc.copy(executeAs = rc.executeAs.orElse(proxyUser))
    }
    val writeConf = {
      val wc = WriteConf.fromSparkConf(conf)
      wc.copy(executeAs = wc.executeAs.orElse(proxyUser))
    }

    val directJoinSetting =
      conf
        .get(DirectJoinSettingParam.name, DirectJoinSettingParam.default)
        .toLowerCase() match {
          case "auto" => Automatic
          case "on" => AlwaysOn
          case "off" => AlwaysOff
          case invalid => throw new IllegalArgumentException(
            s"""
               |$invalid is not a valid ${DirectJoinSettingParam.name} value.
               |${DirectJoinSettingParam.description}""".stripMargin)
    }


    new CassandraSourceRelation(
      tableRef = tableRef,
      userSpecifiedSchema = schema,
      filterPushdown = options.pushdown,
      confirmTruncate = options.confirmTruncate,
      tableSizeInBytes = tableSizeInBytes,
      connector = cassandraConnector,
      readConf = readConf,
      writeConf = writeConf,
      sparkConf = conf,
      sqlContext = sqlContext,
      directJoinSetting = directJoinSetting)
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
    val AllSCCConfNames = (ConfigParameter.names ++ DeprecatedConfigParameter.names)
    //Keyspace/Cluster level settings
    for (prop <- AllSCCConfNames) {
      val value = Seq(
        tableConf.get(prop.toLowerCase(Locale.ROOT)), //tableConf is actually a caseInsensitive map so lower case keys must be used
        sqlConf.get(s"$cluster:$ks/$prop"),
        sqlConf.get(s"$cluster/$prop"),
        sqlConf.get(s"default/$prop"),
        sqlConf.get(prop)).flatten.headOption
      value.foreach(conf.set(prop, _))
    }
    //Set all user properties
    conf.setAll(tableConf -- AllSCCConfNames)
    conf
  }

  private def getProxyUser(sqlContext: SQLContext): Option[String] = {
    val doAsEnabled = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)
    val authenticationEnabled = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION).toUpperCase match {
      case "NONE" => false
      case _ => true
    }
    lazy val user = UserGroupInformation.getCurrentUser

    if (doAsEnabled && authenticationEnabled) {
      Some(user.getUserName)
    } else {
      None
    }
  }

  def setDirectJoin[K: Encoder](ds: Dataset[K], directJoinSetting: DirectJoinSetting = AlwaysOn): Dataset[K] = {
    val oldPlan = ds.queryExecution.logical
    Dataset[K](ds.sparkSession,
      oldPlan.transform{
        case logical @ LogicalRelation(cassandraSourceRelation: CassandraSourceRelation, _, _) =>
          logical.copy(cassandraSourceRelation.withDirectJoin(directJoinSetting))
      }
    )
  }
}

object SolrConstants {
  val DseSolrIndexClassName = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"
  val SolrQuery = "solr_query"
}
