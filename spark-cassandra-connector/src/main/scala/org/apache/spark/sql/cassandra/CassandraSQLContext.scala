package org.apache.spark.sql.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Strategy, SQLContext}


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
  import CassandraSQLContext._

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  @transient
  val sparkConf = sc.getConf

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
