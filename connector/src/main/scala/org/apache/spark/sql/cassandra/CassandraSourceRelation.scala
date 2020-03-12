package org.apache.spark.sql.cassandra

import java.net.InetAddress
import java.util.{Locale, UUID}

import scala.collection.mutable.ListBuffer
import scala.util.Try
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.cassandra.DataTypeConverter._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, ColumnDef, Schema, TableDef}
import com.datastax.spark.connector.rdd.partitioner.DataSizeEstimates
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.forSystemLocalPartitioner
import com.datastax.spark.connector.rdd.{CassandraJoinRDD, CassandraRDD, CassandraTableScanRDD, ReadConf}
import com.datastax.spark.connector.types.{InetType, UUIDType, VarIntType}
import com.datastax.spark.connector.util.Quote._
import com.datastax.spark.connector.util._
import com.datastax.spark.connector.writer.{RowWriterFactory, SqlRowWriter, TTLOption, TimestampOption, WriteConf}
import com.datastax.spark.connector.{SomeColumns, _}

sealed trait DirectJoinSetting
case object AlwaysOn extends DirectJoinSetting
case object AlwaysOff extends DirectJoinSetting
case object Automatic extends DirectJoinSetting

sealed trait DseSearchOptimizationSetting { def enabled = false }
case object On extends DseSearchOptimizationSetting { override def enabled = true }
case object Off extends DseSearchOptimizationSetting
case class Auto(ratio: Double) extends DseSearchOptimizationSetting { override def enabled = true }

trait CassandraTableDefProvider {
  def tableDef: TableDef
  def withSparkConfOption(key: String, value: String): BaseRelation
}

import scala.collection.concurrent.TrieMap



/**
 * Implements [[BaseRelation]]]], [[InsertableRelation]]]] and [[PrunedFilteredScan]]]]
 * It inserts data to and scans Cassandra table. If filterPushdown is true, it pushs down
 * some filters to CQL
 *
 */
case class CassandraSourceRelation(
    tableRef: TableRef,
    userSpecifiedSchema: Option[StructType],
    filterPushdown: Boolean,
    confirmTruncate: Boolean,
    tableSizeInBytes: Option[Long],
    connector: CassandraConnector,
    readConf: ReadConf,
    writeConf: WriteConf,
    sparkConf: SparkConf,
    override val sqlContext: SQLContext,
    directJoinSetting: DirectJoinSetting = Automatic)
  extends BaseRelation
  with InsertableRelation
  with PrunedFilteredScan
  with CassandraTableDefProvider
  with Logging {

  import CassandraSourceRelation._

  //Keep implicits at the top!
  implicit val rwf: RowWriterFactory[Row] = SqlRowWriter.Factory
  implicit val cassandraConnector: CassandraConnector = connector
  implicit val readconf: ReadConf = readConf

  def withDirectJoin(directJoinSetting: DirectJoinSetting) = {
    this.copy(directJoinSetting = directJoinSetting)
  }

  override def withSparkConfOption(key: String, value: String): CassandraSourceRelation = {
    this.copy(sparkConf = sparkConf.clone().set(key, value))
  }

  val tableDef: TableDef = tableFromCassandra(connector,
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

  //Metadata Read Write Fields
  // ignore case
  val regularColumnNames = tableDef.regularColumns.map(_.columnName.toLowerCase())
  val nonRegularColumnNames = (tableDef.clusteringColumns ++ tableDef.partitionKey).map(_.columnName.toLowerCase)
  val ignoreMissingMetadataColumns: Boolean = sparkConf.getBoolean(CassandraSourceRelation.IgnoreMissingMetaColumns.name,
    CassandraSourceRelation.IgnoreMissingMetaColumns.default)

  def checkMetadataColumn(columnName: String, function: String): Boolean = {
    val lowerCaseName = columnName.toLowerCase
    def metadataError (errorType: String) = {
      throw new IllegalArgumentException(s"Cannot lookup $function on $errorType column $columnName")
    }

    if(nonRegularColumnNames.contains(lowerCaseName)) metadataError("non-regular")
    if(regularColumnNames.contains(lowerCaseName)) true
    else if(ignoreMissingMetadataColumns) false
    else metadataError("missing")
  }

  private val writeTimeFields =
    sparkConf
      .getAllWithPrefix(WriteTimeParam.name + ".")
      .filter { case (columnName: String, writeTimeName: String) => checkMetadataColumn(columnName, "writetime")}
      .map{ case (columnName: String, writeTimeName: String) =>
        val colDef = tableDef.columnByNameIgnoreCase(columnName)
        val colType = if (colDef.isMultiCell)
          ArrayType(LongType) else LongType
        (colDef.columnName, StructField(writeTimeName, colType, nullable = false))}

  private val ttlFields =
    sparkConf
      .getAllWithPrefix(TTLParam.name + ".")
      .filter { case (columnName: String, writeTimeName: String) => checkMetadataColumn(columnName, "ttl")}
      .map{ case (columnName: String, ttlName: String) =>
        val colDef = tableDef.columnByNameIgnoreCase(columnName)
        val colType = if (colDef.isMultiCell)
          ArrayType(IntegerType) else IntegerType
        (colDef.columnName, StructField(ttlName, colType, nullable = true))}

  private val ttlWriteOption =
    sparkConf.getOption(TTLParam.name)
      .map( value =>
        Try(value.toInt)
          .map(TTLOption.constant)
          .getOrElse(TTLOption.perRow(value)))
      .getOrElse(writeConf.ttl)

  private val timestampWriteOption =
    sparkConf.getOption(WriteTimeParam.name)
      .map( value =>
        Try(value.toLong)
          .map(TimestampOption.constant)
          .getOrElse(TimestampOption.perRow(value)))
      .getOrElse(writeConf.timestamp)

  private val metadataReadColumnsMap =
    (ttlFields.map(field => (field._2.name, TTL(field._1, Some(field._2.name)))) ++
      writeTimeFields.map(field => (field._2.name, WriteTime(field._1, Some(field._2.name))))).toMap

  private val metadataColumnNames =
    ttlFields.map(field => field._2.name) ++
      writeTimeFields.map(field => field._2.name) ++
      sparkConf.getOption(WriteTimeParam.name).toSeq ++
      sparkConf.getOption(TTLParam.name).toSeq
  //End Metadata Fields

  override def schema: StructType = {
    userSpecifiedSchema.getOrElse(
      StructType(tableDef.columns.map(toStructField)
        ++ writeTimeFields.map(_._2)
        ++ ttlFields.map(_._2)
      ))
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

    val metadataEnrichedWriteConf = writeConf.copy(
      ttl = ttlWriteOption,
      timestamp = timestampWriteOption)

    val columns = SomeColumns(data.columns.map(x => x: ColumnRef): _*)
    val converter = CatalystTypeConverters.createToScalaConverter(data.schema)
    data
      .queryExecution.toRdd
      .map(converter(_).asInstanceOf[Row])
      .saveToCassandra(tableRef.keyspace, tableRef.table, columns, metadataEnrichedWriteConf)
  }

  override def sizeInBytes: Long = {
    // If it's not found, use SQLConf default setting
    tableSizeInBytes.getOrElse(sqlContext.conf.defaultSizeInBytes)
  }

  private[this] val baseRdd =
    sqlContext.sparkContext.cassandraTable[CassandraSQLRow](tableRef.keyspace, tableRef.table)

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filterPushdown match {
    case true => predicatePushDown(filters).handledBySpark.toArray
    case false => filters
  }

  /** Looks for a given key in SparkConf, if the key is not found, it looks in LocalProperties.
    * Designed so we can set this in testing to different values without making a new context check local property
    * as well */
  private def getConfigParameter(key: String): Option[String] = {
    val sc = sqlContext.sparkContext
    sc.getConf.getOption(key).orElse(Option(sc.getLocalProperty(key)))
  }

  lazy val additionalRules: Seq[CassandraPredicateRules] = {
    val userClasses = getConfigParameter(AdditionalCassandraPushDownRulesParam.name)

    userClasses match {
      case Some(classes) =>
        classes
          .trim
          .split("""\s*,\s*""")
          .map(ReflectionUtil.findGlobalObject[CassandraPredicateRules])
      case None => AdditionalCassandraPushDownRulesParam.default
    }
  }

  /*
  Eliminate duplicate calculation of predicate pushdowns
  (once for unhandled filters and once for actually building the scan)
  */
  val pushdownCache: TrieMap[Seq[Filter], AnalyzedPredicates] = TrieMap.empty

  private def solrPredicateRules: Option[CassandraPredicateRules] = {
    if (searchOptimization.enabled) {
      logDebug(s"Search Optimization Enabled - $searchOptimization")
      Some(new SolrPredicateRules(searchOptimization))
    } else {
      None
    }
  }

  private def predicatePushDown(filters: Array[Filter]): AnalyzedPredicates = pushdownCache.getOrElseUpdate(filters.toSeq, {

    logDebug(s"Input Predicates: [${filters.mkString(", ")}]")

    val pv = connector.withSessionDo(_.getContext.getProtocolVersion)

    /** Apply built in rules **/
    val bcpp = new BasicCassandraPredicatePushDown(filters.toSet, tableDef, pv)
    val basicPushdown = AnalyzedPredicates(bcpp.predicatesToPushDown, bcpp.predicatesToPreserve)

    logDebug(s"Basic Rules Applied:\n$basicPushdown")

    val predicatePushDownRules = Seq(
      DsePredicateRules,
      InClausePredicateRules) ++
      solrPredicateRules ++
      additionalRules

    /** Apply non-basic rules **/
    val finalPushdown = predicatePushDownRules.foldLeft(basicPushdown)(
      (pushdowns, rules) => {
        val pd = rules(pushdowns, tableDef, sparkConf)
        logDebug(s"Applied ${rules.getClass.getSimpleName} Pushdown Filters:\n$pd")
        pd
      }
    )

    logDebug(s"Final Pushdown filters:\n$finalPushdown")
    finalPushdown
  })

  /** Preserves `columns` order */
  private def eqAndInColumnFilters(columns: Seq[ColumnDef], predicates: AnalyzedPredicates): Seq[Filter] = {
    val predicatesByColumnName = predicates.handledByCassandra.collect {
      case eq @ EqualTo(column, _) => (column, eq)
      case in @ In(column, _) => (column, in)
    }.toMap
    columns.flatMap(column => predicatesByColumnName.get(column.columnName))
  }

  /** Is convertable to joinWithCassandraTable if query
    * - uses all partition key columns
    * - spans multiple partitions
    * - contains IN key values and the cartesian set of those values is greater than threshold
    */
  private def isConvertableToJoinWithCassandra(predicates: AnalyzedPredicates): Boolean = {
    val inClauseConversionThreshold = getConfigParameter(InClauseToJoinWithTableConversionThreshold.name).map(_.toLong)
        .getOrElse(InClauseToJoinWithTableConversionThreshold.default)
    if (inClauseConversionThreshold == 0L) {
      false
    } else {
      val partitionFilters = eqAndInColumnFilters(tableDef.partitionKey, predicates)
      val clusteringFilters = eqAndInColumnFilters(tableDef.clusteringColumns, predicates)
      val inClauseValuesCartesianSize = (partitionFilters ++ clusteringFilters).foldLeft(1L) {
        case (cartSize, In(_, values)) => cartSize * values.length
        case (cartSize, _) => cartSize
      }
      partitionFilters.exists(_.isInstanceOf[In]) &&
        tableDef.partitionKey.length == partitionFilters.length &&
        inClauseValuesCartesianSize >= inClauseConversionThreshold
    }
  }

  private def joinKeysRDD(columnNameAndValues: Seq[(String, Array[AnyRef])]) = {
    val metadata = CassandraRowMetadata.fromColumnNames(columnNameAndValues.map(_._1))
    val parallelism = sqlContext.sparkContext.defaultParallelism
    val keysRDDs = for ((_, values) <- columnNameAndValues) yield sqlContext.sparkContext.parallelize(values, parallelism)
    val headRDD = keysRDDs.head.map(ListBuffer(_))

    keysRDDs.tail.foldLeft(headRDD) { (cartesian, nextRDD) =>
      cartesian
        .cartesian(nextRDD)
        .map { case (inValues, nextValue) => inValues += nextValue }
        .coalesce(parallelism, shuffle = false)
    }.map(values => new CassandraRow(metadata, values.toIndexedSeq))
  }

  private def joinWithCassandraByPredicates(analyzedPredicates: AnalyzedPredicates, requiredColumns: Array[String]) = {
    val primaryKeyFilters = eqAndInColumnFilters(tableDef.primaryKey, analyzedPredicates)
    val columnNameAndValues = primaryKeyFilters.collect {
      case eq@EqualTo(column, _) => (column, Array(eq.value.asInstanceOf[AnyRef]))
      case in@In(column, _) => (column, in.values.asInstanceOf[Array[AnyRef]])
    }
    val joinColumns = tableDef.primaryKey
      .takeWhile(c => columnNameAndValues.exists(cv => cv._1 == c.columnName))
      .map(_.ref)
    val selectColumns =
      if (requiredColumns.isEmpty) AllColumns
      else SomeColumns(requiredColumns.map(column => column: ColumnRef): _*)

    val join = new CassandraJoinRDD[CassandraRow, CassandraSQLRow](
      left = joinKeysRDD(columnNameAndValues),
      keyspaceName = tableDef.keyspaceName,
      tableName = tableDef.tableName,
      joinColumns = SomeColumns(joinColumns: _*),
      columnNames = selectColumns,
      readConf = readconf,
      connector = connector)
    maybePushdownFilters(join, (analyzedPredicates.handledByCassandra -- primaryKeyFilters.toSet).toSeq).map(_._2)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (filterPushdown) {
      val analyzedPredicates = predicatePushDown(filters)
      logDebug(s"Building RDD with filters:\n$analyzedPredicates")

      if (isConvertableToJoinWithCassandra(analyzedPredicates)) {
        logInfo(s"Number of keys in 'IN' clauses exceeds ${InClauseToJoinWithTableConversionThreshold.name}, " +
          s"converting to joinWithCassandraTable.")
        joinWithCassandraByPredicates(analyzedPredicates, requiredColumns).asInstanceOf[RDD[Row]]
      } else {
        val filteredRdd = maybePushdownFilters(baseRdd, analyzedPredicates.handledByCassandra.toSeq)
        maybeSelect(filteredRdd, requiredColumns)
      }
    } else {
      maybeSelect(baseRdd, requiredColumns)
    }
  }

  /** Define a type for CassandraRDD[CassandraSQLRow]. It's used by following methods */
  private type RDDType = CassandraRDD[CassandraSQLRow]

  /** Transfer selection to limit to columns specified */
  private def maybeSelect(rdd: RDDType, requiredColumns: Array[String]) : RDD[Row] = {

    //Get all required ColumnRefs, MetaDataRefs should be picked out of the ReadColumnsMap
    val requiredCassandraColumns = requiredColumns.map(columnName =>
      metadataReadColumnsMap.getOrElse(columnName, columnName: ColumnRef)
    )

    val prunedRdd = if (requiredCassandraColumns.nonEmpty) {
      rdd.select(requiredCassandraColumns: _*)
    } else {
      //No Columns Selected - Count Optimizations
      rdd match {
        case rdd: CassandraTableScanRDD[_] =>
          val tableIsSolrIndexed =
            rdd.tableDef
              .indexes
              .exists(index => index.className.contains(SolrConstants.DseSolrIndexClassName))
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
  private def maybePushdownFilters[T](rdd: CassandraRDD[T], filters: Seq[Filter]) : CassandraRDD[T]= {
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

  val WriteTimeParam = ConfigParameter[Option[String]](
    name = "writetime",
    section = ReferenceSection,
    default = None,
    description =
      """Surfaces the Cassandra Row Writetime as a Column
        |with the named specified. When reading use writetime.columnName=aliasForWritetime. This
        |can be done for every column with a writetime. When Writing use writetime=columnName and the
        |columname will be used to set the writetime for that row.""".stripMargin
  )

  val TTLParam = ConfigParameter[Option[String]](
    name = "ttl",
    section = ReferenceSection,
    default = None,
    description =
      """Surfaces the Cassandra Row TTL as a Column
        |with the named specified. When reading use ttl.columnName=aliasForTTL. This
        |can be done for every column with a TTL. When writing use writetime=columnName and the
        |columname will be used to set the TTL for that row.""".stripMargin
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

  val InClauseToJoinWithTableConversionThreshold = ConfigParameter[Long](
    name = "spark.sql.dse.inClauseToJoinConversionThreshold",
    section = DseReferenceSection,
    default = 2500L,
    description =
        s"""Queries with `IN` clause(s) are converted to JoinWithCassandraTable operation if the size of cross
           |product of all `IN` value sets exceeds this value. To disable `IN` clause conversion, set this setting to 0.
           |Query `select * from t where k1 in (1,2,3) and k2 in (1,2) and k3 in (1,2,3,4)` has 3 sets of `IN` values.
           |Cross product of these values has size of 24.
         """.stripMargin
  )

  val InClauseToFullTableScanConversionThreshold = ConfigParameter[Long](
    name = "spark.sql.dse.inClauseToFullScanConversionThreshold",
    section = DseReferenceSection,
    default = 20000000L,
    description =
        s"""Queries with `IN` clause(s) are not converted to JoinWithCassandraTable operation if the size of cross
           |product of all `IN` value sets exceeds this value. It is meant to stop conversion for huge `IN` values sets
           |that may cause memory problems. If this limit is exceeded full table scan is performed.
           |This setting takes precedence over ${InClauseToJoinWithTableConversionThreshold.name}.
           |Query `select * from t where k1 in (1,2,3) and k2 in (1,2) and k3 in (1,2,3,4)` has 3 sets of `IN` values.
           |Cross product of these values has size of 24.
         """.stripMargin
  )

  val IgnoreMissingMetaColumns = ConfigParameter[Boolean] (
    name = "ignoreMissingMetaColumns",
    section = DseReferenceSection,
    default = false,
    description =
      s"""Acceptable values, "true", "false"
         |"true" ignore missing meta properties
         |"false" throw error if missing property is requested
      """.stripMargin
  )
  val defaultClusterName = "default"

  private val proxyPerSourceRelationEnabled = sys.env.getOrElse("DSE_ENABLE_PROXY_PER_SRC_RELATION", "false").toBoolean

  def apply(
    tableRef: TableRef,
    sqlContext: SQLContext,
    options: CassandraSourceOptions,
    schema : Option[StructType]) : CassandraSourceRelation = {

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
        case logical @ LogicalRelation(cassandraSourceRelation: CassandraSourceRelation, _, _, _) =>
          logical.copy(cassandraSourceRelation.withDirectJoin(directJoinSetting))
      }
    )
  }
}

object SolrConstants {
  val DseSolrIndexClassName = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"
  val SolrQuery = "solr_query"
}
