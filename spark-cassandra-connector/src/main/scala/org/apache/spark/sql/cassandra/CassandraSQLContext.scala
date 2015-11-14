package org.apache.spark.sql.cassandra

import java.util.NoSuchElementException

import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlanner => NewSparkPlanner}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.{DataFrame, SQLContext, Strategy}

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
  import org.apache.spark.sql.cassandra.CassandraSQLContext._

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  /** Set default Cassandra keyspace to be used when accessing tables with unqualified names. */
  def setKeyspace(ks: String) = {
    this.setConf(CassandraSqlKSNameProperty, ks)
  }

  /** Set current used database name. Database is equivalent to keyspace */
  def setDatabase(db: String) = setKeyspace(db)

  /** Set current used cluster name */
  def setCluster(cluster: String) = {
    this.setConf(CassandraSqlClusterNameProperty, cluster)
  }

  /** Get current used cluster name */
  def getCluster : String = this.getConf(CassandraSqlClusterNameProperty, defaultClusterName)

  /**
   * Returns keyspace/database set previously by [[setKeyspace]] or throws IllegalStateException if
   * keyspace has not been set yet.
   */
  def getKeyspace: String = {
    try {
      this.getConf(CassandraSqlKSNameProperty)
    } catch {
      case _: NoSuchElementException =>
        throw new IllegalStateException("Default keyspace not set. Please call CassandraSqlContext#setKeyspace.")
    }
  }

  /** Executes SQL query against Cassandra and returns DataFrame representing the result. */
  def cassandraSql(cassandraQuery: String): DataFrame = new DataFrame(this, super.parseSql(cassandraQuery))

  /** Delegates to [[cassandraSql]] */
  override def sql(cassandraQuery: String): DataFrame = cassandraSql(cassandraQuery)

  /** A catalyst metadata catalog that points to Cassandra. */
  @transient
  override protected[sql] lazy val catalog = new CassandraCatalog(this)

  /** Modified Catalyst planner that does Cassandra-specific predicate pushdown */
  @transient
  override protected[sql] val planner = new NewSparkPlanner(this) {
    val cassandraContext = CassandraSQLContext.this

    sqlContext.experimental.extraStrategies ++ (
      DataSourceStrategy ::
        DDLStrategy ::
        TakeOrderedAndProject ::
        Aggregation ::
        LeftSemiJoin ::
        EquiJoinSelection ::
        InMemoryScans ::
        BasicOperators ::
        BroadcastNestedLoop ::
        CartesianProduct ::
        DefaultJoin :: Nil)
  }
}

object CassandraSQLContext {
  // Should use general used database than Cassandra specific keyspace?
  // Other source tables don't have keyspace concept. We should make
  // an effort to set CassandraSQLContext a more database like to join
  // tables from other sources. Keyspace is equivalent to database in SQL world
  val CassandraSqlKSNameProperty = "spark.cassandra.sql.keyspace"
  val CassandraSqlClusterNameProperty = "spark.cassandra.sql.cluster"

  val Properties = Seq(
    CassandraSqlKSNameProperty,
    CassandraSqlClusterNameProperty
  )
}
