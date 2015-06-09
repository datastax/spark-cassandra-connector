package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Strategy, SQLContext, SchemaRDD}

import collection.mutable

/** Allows to execute SQL queries against Cassandra and access results as
  * `SchemaRDD` collections. Predicate pushdown to Cassandra is supported.
  *
  * Example:
  * {{{
  *   import com.datastax.spark.connector._
  *
  *   val sparkMasterHost = "127.0.0.1"
  *   val cassandraHost = "127.0.0.1"
  *
  *   // Tell Spark the address of one Cassandra node:
  *   val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
  *
  *   // Connect to the Spark cluster:
  *   val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "example", conf)
  *
  *   // Create CassandraSQLContext:
  *   val cc = new CassandraSQLContext(sc)
  *
  *   // Execute SQL query:
  *   val rdd = cc.sql("SELECT * FROM keyspace.table ...")
  *
  * }}} */
class CassandraSQLContext(sc: SparkContext) extends SQLContext(sc) {
  import CassandraSQLContext._

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  @transient
  val sparkConf = sc.getConf

  @transient
  private val clusterReadConf = mutable.Map[String, ReadConf]()
  @transient
  private val keyspaceReadConf = mutable.Map[Seq[String], ReadConf]()
  @transient
  private val tableReadConf = mutable.Map[Seq[String], ReadConf]()

  /** Add table level read configuration settings. Set cluster to None for a single cluster */
  def addTableReadConf(keyspace: String, table: String, conf: SparkConf, cluster: Option[String]): CassandraSQLContext = {
    addTableReadConf(keyspace, table, ReadConf.fromSparkConf(conf), cluster)
  }

  /** Add table level read configuration settings. Set cluster to None for a single cluster */
  def addTableReadConf(keyspace: String, table: String, conf: ReadConf, cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableReadConf += Seq(table, keyspace, c) -> conf
      case _       => tableReadConf += Seq(table, keyspace) -> conf
    }
    this
  }

  /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelReadConf(keyspace: String, conf: SparkConf, cluster: Option[String]): CassandraSQLContext = {
    addKeyspaceLevelReadConf(keyspace, ReadConf.fromSparkConf(conf), cluster)
  }

  /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelReadConf(keyspace: String, conf: ReadConf, cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceReadConf += Seq(keyspace, c) -> conf
      case _       => keyspaceReadConf += Seq(keyspace) -> conf
    }
    this
  }

  /** Add cluster level read configuration settings */
  def addClusterLevelReadConf(cluster: String, conf: SparkConf): CassandraSQLContext = {
    addClusterLevelReadConf(cluster, ReadConf.fromSparkConf(conf))
  }

  /** Add cluster level read configuration settings */
  def addClusterLevelReadConf(cluster: String, conf: ReadConf) = {
    validateClusterName(cluster)
    clusterReadConf += cluster -> conf
    this
  }

  /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getReadConf(keyspace: String, table: String, cluster: Option[String]): ReadConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
                      tableReadConf.get(Seq(table, keyspace, c)).getOrElse(
                        keyspaceReadConf.get(Seq(keyspace, c)).getOrElse(
                          clusterReadConf.get(c).getOrElse(ReadConf.fromSparkConf(sparkConf))))
      case _       => tableReadConf.get(Seq(table, keyspace)).getOrElse(
                        keyspaceReadConf.get(Seq(keyspace)).getOrElse(ReadConf.fromSparkConf(sparkConf)))
    }
  }

  @transient
  private val clusterWriteConf = mutable.Map[String, WriteConf]()
  @transient
  private val keyspaceWriteConf = mutable.Map[Seq[String], WriteConf]()
  @transient
  private val tableWriteConf = mutable.Map[Seq[String], WriteConf]()

  /** Add table level write configuration settings. Set cluster to None for a single cluster */
  def addTableWriteConf(keyspace: String, table: String, conf: SparkConf, cluster: Option[String]): CassandraSQLContext = {
    addTableWriteConf(keyspace, table, WriteConf.fromSparkConf(conf), cluster)
  }

  /** Add table level write configuration settings. Set cluster to None for a single cluster */
  def addTableWriteConf(keyspace: String, table: String, conf: WriteConf, cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableWriteConf += Seq(table, keyspace, c) -> conf
      case _       => tableWriteConf += Seq(table, keyspace) -> conf
    }
    this
  }

  /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelWriteConf(keyspace: String, conf: SparkConf, cluster: Option[String]): CassandraSQLContext = {
    addKeyspaceLevelWriteConf(keyspace, WriteConf.fromSparkConf(conf), cluster)
  }

  /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelWriteConf(keyspace: String, writeConf: WriteConf, cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceWriteConf += Seq(keyspace, c) -> writeConf
      case _       => keyspaceWriteConf += Seq(keyspace) -> writeConf
    }
    this
  }

  /** Add cluster level write configuration settings */
  def addClusterLevelWriteConf(cluster: String, conf: SparkConf): CassandraSQLContext = {
    addClusterLevelWriteConf(cluster, WriteConf.fromSparkConf(conf))
  }

  /** Add cluster level write configuration settings */
  def addClusterLevelWriteConf(cluster: String, conf: WriteConf) = {
    validateClusterName(cluster)
    clusterWriteConf += cluster -> conf
    this
  }

  /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getWriteConf(keyspace: String, table: String, cluster: Option[String]): WriteConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
                      tableWriteConf.get(Seq(table, keyspace, c)).getOrElse(
                        keyspaceWriteConf.get(Seq(keyspace, c)).getOrElse(
                          clusterWriteConf.get(c).getOrElse(WriteConf.fromSparkConf(sparkConf))))
      case _       => tableWriteConf.get(Seq(table, keyspace)).getOrElse(
                        keyspaceWriteConf.get(Seq(keyspace)).getOrElse(WriteConf.fromSparkConf(sparkConf)))
    }
  }

  @transient
  private val clusterCassandraConnConf = mutable.Map[String, CassandraConnectorConf]()

  /** Add cluster level write configuration settings */
  def addClusterLevelCassandraConnConf(cluster: String, conf: SparkConf): CassandraSQLContext = {
    addClusterLevelCassandraConnConf(cluster, CassandraConnectorConf(conf))
  }

  /** Add cluster level write configuration settings */
  def addClusterLevelCassandraConnConf(cluster: String, conf: CassandraConnectorConf) = {
    validateClusterName(cluster)
    clusterCassandraConnConf += cluster -> conf
    this
  }

  /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
  def getCassandraConnConf(cluster: Option[String]): CassandraConnectorConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
                      clusterCassandraConnConf.get(c).getOrElse(throw new RuntimeException(s"Missing cluster $c Cassandra connection conf"))
      case _       => CassandraConnectorConf(sparkConf)
    }
  }

  private def validateClusterName(cluster: String) {
    if (StringUtils.isEmpty(cluster)) {
      throw new IllegalArgumentException("cluster name can't be null or empty")
    }
  }

  private var keyspaceName = sparkConf.getOption(CassandraSQLKeyspaceNameProperty)

  /** Sets default Cassandra keyspace to be used when accessing tables with unqualified names. */
  def setKeyspace(ks: String) {
    keyspaceName = Some(ks)
  }

  /** Returns keyspace set previously by [[setKeyspace]] or throws IllegalStateException if
    * keyspace has not been set yet. */
  def getKeyspace: String = keyspaceName.getOrElse(
    throw new IllegalStateException("Default keyspace not set. Please call CassandraSqlContext#setKeyspace."))

  /** Executes SQL query against Cassandra and returns DataFrame representing the result. */
  def cassandraSql(cassandraQuery: String): DataFrame = new DataFrame(this, super.parseSql(cassandraQuery))

  /** Delegates to [[cassandraSql]] */
  override def sql(cassandraQuery: String): DataFrame = cassandraSql(cassandraQuery)

  /** A catalyst metadata catalog that points to Cassandra. */
  @transient
  override protected[sql] lazy val catalog = new CassandraCatalog(this) with OverrideCatalog

  /** Modified Catalyst planner that does Cassandra-specific predicate pushdown */
  @transient
  override protected[sql] val planner = new SparkPlanner with CassandraStrategies {
    val cassandraContext = CassandraSQLContext.this
    override val strategies: Seq[Strategy] = Seq(
      DDLStrategy,
      TakeOrdered,
      ParquetOperations,
      InMemoryScans,
      CassandraTableScans,
      DataSinks,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }
}

object CassandraSQLContext {
  val CassandraSQLKeyspaceNameProperty = "spark.cassandra.keyspace"

  val Properties = Seq(
    CassandraSQLKeyspaceNameProperty
  )
}
