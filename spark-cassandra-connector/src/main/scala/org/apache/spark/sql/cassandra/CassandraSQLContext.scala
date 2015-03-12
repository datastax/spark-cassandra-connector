package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.rdd.ReadConf
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Strategy, SQLContext, SchemaRDD}

import collection.mutable.Map

/** Allows to execute SQL queries against Cassandra and access results as
  * [[org.apache.spark.sql.SchemaRDD]] collections.
  * Predicate pushdown to Cassandra is supported.
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

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  @transient
  val conf = sc.getConf

  @transient
  private val clusterReadConf: Map[String, SparkConf] = Map()
  @transient
  private val keyspaceReadConf: Map[Seq[String], SparkConf] = Map()
  @transient
  private val tableReadConf: Map[Seq[String], SparkConf] = Map()

  /** Add table level read configuration settings */
  def addTableReadConf(keyspace: String, table: String, conf: SparkConf, cluster: Option[String]) = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          tableReadConf += Seq(table, keyspace, c) -> conf
        }
      case _ => tableReadConf += Seq(table, keyspace) -> conf
    }
    this
  }

  /** Add keyspace level read configuration settings */
  def addKeyspaceLevelReadConf(keyspace: String, conf: SparkConf, cluster: Option[String] = None) = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          keyspaceReadConf += Seq(keyspace, c) -> conf
        }
      case _ => keyspaceReadConf += Seq(keyspace) -> conf
    }
    this
  }

  /** Add cluster level read configuration settings */
  def addClusterLevelReadConf(cluster: String, conf: SparkConf) = {
    if (StringUtils.isEmpty(cluster)) {
      throw new IllegalArgumentException("cluster name can't be null or empty")
    }
    clusterReadConf += cluster -> conf
    this
  }

  /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getReadConf(keyspace: String, table: String, cluster: Option[String]): SparkConf = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          tableReadConf.get(Seq(table, keyspace, c)).getOrElse(
            keyspaceReadConf.get(Seq(keyspace, c)).getOrElse(
              clusterReadConf.get(c).getOrElse(conf)))
        }
      case _ => tableReadConf.get(Seq(table, keyspace)).getOrElse(
        keyspaceReadConf.get(Seq(keyspace)).getOrElse(conf))
    }
  }

  @transient
  private val clusterWriteConf: Map[String, SparkConf] = Map()
  @transient
  private val keyspaceWriteConf: Map[Seq[String], SparkConf] = Map()
  @transient
  private val tableWriteConf: Map[Seq[String], SparkConf] = Map()

  /** Add table level write configuration settings */
  def addTableWriteConf(keyspace: String, table: String, conf: SparkConf, cluster: Option[String]) = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          tableWriteConf += Seq(table, keyspace, c) -> conf
        }
      case _ => tableWriteConf += Seq(table, keyspace) -> conf
    }
    this
  }

  /** Add keyspace level write configuration settings */
  def addKeyspaceLevelWriteConf(keyspace: String, conf: SparkConf, cluster: Option[String] = None) = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          keyspaceWriteConf += Seq(keyspace, c) -> conf
        }
      case _ => keyspaceWriteConf += Seq(keyspace) -> conf
    }
    this
  }

  /** Add cluster level write configuration settings */
  def addClusterLevelWriteConf(cluster: String, conf: SparkConf) = {
    if (StringUtils.isEmpty(cluster)) {
      throw new IllegalArgumentException("cluster name can't be null or empty")
    }
    clusterWriteConf += cluster -> conf
    this
  }

  /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getWriteConf(keyspace: String, table: String, cluster: Option[String]): SparkConf = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          tableWriteConf.get(Seq(table, keyspace, c)).getOrElse(
            keyspaceWriteConf.get(Seq(keyspace, c)).getOrElse(
              clusterWriteConf.get(c).getOrElse(conf)))
        }
      case _ => tableWriteConf.get(Seq(table, keyspace)).getOrElse(
        keyspaceWriteConf.get(Seq(keyspace)).getOrElse(conf))
    }
  }

  @transient
  private val clusterCassandraConnConf: Map[String, SparkConf] = Map()

  /** Add cluster level write configuration settings */
  def addClusterLevelCassandraConnConf(cluster: String, conf: SparkConf) = {
    if (StringUtils.isEmpty(cluster)) {
      throw new IllegalArgumentException("cluster name can't be null or empty")
    }
    clusterCassandraConnConf += cluster -> conf
    this
  }

  /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
  def getCassandraConnConf(cluster: Option[String]): SparkConf = {
    cluster match {
      case Some(c) =>
        if (StringUtils.isEmpty(c)) {
          throw new IllegalArgumentException("cluster name can't be null or empty")
        } else {
          clusterCassandraConnConf.get(c).getOrElse(
            throw new RuntimeException(s"Missing cluster $c Cassandra connection conf"))
        }
      case _ => conf
    }
  }

  private var keyspaceName = conf.getOption("spark.cassandra.keyspace")

  /** Sets default Cassandra keyspace to be used when accessing tables with unqualified names. */
  def setKeyspace(ks: String) {
    keyspaceName = Some(ks)
  }

  /** Returns keyspace set previously by [[setKeyspace]] or throws IllegalStateException if
    * keyspace has not been set yet. */
  def getKeyspace: String = keyspaceName.getOrElse(
    throw new IllegalStateException("Default keyspace not set. Please call CassandraSqlContext#setKeyspace."))

  /** Executes SQL query against Cassandra and returns SchemaRDD representing the result. */
  def cassandraSql(cassandraQuery: String): SchemaRDD = new SchemaRDD(this, super.parseSql(cassandraQuery))

  /** Delegates to [[cassandraSql]] */
  override def sql(cassandraQuery: String): SchemaRDD = cassandraSql(cassandraQuery)

  /** A catalyst metadata catalog that points to Cassandra. */
  @transient
  override protected[sql] lazy val catalog = new CassandraCatalog(this) with OverrideCatalog

  /** Modified Catalyst planner that does Cassandra-specific predicate pushdown */
  @transient
  override protected[sql] val planner = new SparkPlanner with CassandraStrategies {
    val cassandraContext = CassandraSQLContext.this
    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(CassandraSQLContext.this),
      TakeOrdered,
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
