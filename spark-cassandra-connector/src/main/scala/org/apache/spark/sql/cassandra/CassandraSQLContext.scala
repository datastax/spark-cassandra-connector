package org.apache.spark.sql.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.DataSourceStrategy
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Strategy, SQLContext}

/** Allows to execute SQL queries against Cassandra and access results as
  * [[org.apache.spark.sql.DataFrame]] collections.
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

  private var keyspaceName = Option(conf.getConf(CassandraSQLKeyspaceNameProperty, null))

  /** Sets default Cassandra keyspace to be used when accessing tables with unqualified names. */
  def setKeyspace(ks: String) {
    keyspaceName = Some(ks)
  }

  /** Returns keyspace set previously by [[setKeyspace]] or throws IllegalStateException if
    * keyspace has not been set yet. */
  def getKeyspace: String = keyspaceName.getOrElse(
    throw new IllegalStateException("Default keyspace not set. Please call CassandraSQLContext#setKeyspace."))

  /** Executes SQL query against Cassandra and returns DataFrame representing the result. */
  def cassandraSql(cassandraQuery: String): DataFrame = new DataFrame(this, super.parseSql(cassandraQuery))

  /** Delegates to [[cassandraSql]] */
  override def sql(cassandraQuery: String): DataFrame = cassandraSql(cassandraQuery)

  /** A catalyst metadata catalog that points to Cassandra. */
  @transient
  override protected[sql] lazy val catalog = new CassandraCatalog(this)

  /** Modified Catalyst planner that does Cassandra-specific predicate pushdown */
  @transient
  override protected[sql] val planner = new SparkPlanner with CassandraStrategies {
    val cassandraContext = CassandraSQLContext.this
    override val strategies: Seq[Strategy] = Seq(
      DataSourceStrategy,
      CassandraDDLStrategy,
      DDLStrategy,
      TakeOrdered,
      ParquetOperations,
      InMemoryScans,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  /**
   * Get all tables for given database name and cluster.
   * databaseName is equivalent to keyspace as a Cassandra name
   */
  def getTables(databaseName: Option[String], cluster: Option[String] = None): Seq[(String, Boolean)] =
    catalog.getTables(databaseName, cluster)

  /**
   * Only register table to local cache. To register table in metastore, use
   * registerTable(tableIdent, source, schema, options) method
   */
  def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit =
    catalog.registerTable(tableIdentifier, plan)

  /** Register a customized table meta data to local cache and metastore */
  def registerTable(
      tableIdent: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit =
    catalog.registerTable(tableIdent, source, schema, options)

  /** Unregister table from local cache and metastore. */
  def unregisterTable(tableIdent: TableIdent): Unit = catalog.unregisterTable(tableIdent)

  /** Unregister all tables from local cache and metastore. */
  def unregisterAllTables(): Unit = catalog.unregisterAllTables()

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  def refreshTable(tableIdent: TableIdent): Unit = catalog.refreshTable(tableIdent)

  /** Check whether table exists. It's either in Cassandra or in metastore */
  def tableExists(tableIdent: TableIdent): Boolean = catalog.tableExists(tableIdent)

  /** Check whether table is stored in metastore */
  def tableExistsInMetastore(tableIdent: TableIdent): Boolean = catalog.tableExistsInMetastore(tableIdent)
}

object CassandraSQLContext {
  val CassandraSQLKeyspaceNameProperty = "spark.cassandra.keyspace"

  val Properties = Seq(
    CassandraSQLKeyspaceNameProperty
  )
}
