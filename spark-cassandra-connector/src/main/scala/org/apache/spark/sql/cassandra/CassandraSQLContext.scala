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

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  /** Sets default Cassandra keyspace to be used when accessing tables with unqualified names. */
  def setKeyspace(ks: String) = this.useDatabase(ks)

  /** Returns keyspace set previously by [[setKeyspace]] or throws IllegalStateException if
    * keyspace has not been set yet. */
  def getKeyspace: String = this.getDatabase

  /** Executes SQL query against Cassandra and returns DataFrame representing the result. */
  def cassandraSql(cassandraQuery: String): DataFrame =
    new DataFrame(this, parseSql(cassandraQuery))

  @transient
  protected[sql] val cassandraDdlParser = new CassandraDDLParser(sqlParser.apply(_))

  override protected[sql] def parseSql(sql: String): LogicalPlan = {
    cassandraDdlParser(sql, false)
      .getOrElse(ddlParser(sql, false)
      .getOrElse(sqlParser(sql)))
  }

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
   * Get all databases for given cluster name.
   * database is equivalent to keyspace
   */
  def getDatabases(cluster: Option[String] = None): Seq[String] =
    catalog.getDatabases(cluster)

  /** Get all clusters */
  def getClusters(): Seq[String] = catalog.getClusters()

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
  def unregisterTable(tableIdent: TableIdent): Unit =
    catalog.unregisterTable(tableIdent)

  /** Unregister all tables from local cache and metastore. */
  def unregisterAllTables(): Unit = catalog.unregisterAllTables()

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  def refreshTable(tableIdent: TableIdent): Unit =
    catalog.refreshTable(tableIdent)

  /** Check whether table exists. It's either in Cassandra or in metastore */
  def tableExists(tableIdent: TableIdent): Boolean =
    catalog.tableExists(tableIdent)

  /** Check whether table is stored in metastore */
  def tableExistsInMetastore(tableIdent: TableIdent): Boolean =
    catalog.tableExistsInMetastore(tableIdent)

  /** Create a database in metastore */
  def createDatabase(database: String, cluster: Option[String]): Unit =
    catalog.createDatabase(database, cluster)

  /** Create a cluster in metastore */
  def createCluster(cluster: String): Unit = catalog.createCluster(cluster)

  /** Unregister database from local cache and metastore. */
  def unregisterDatabase(database: String, cluster: Option[String]): Unit =
    catalog.unregisterDatabase(database, cluster)

  /** Unregister cluster from local cache and metastore. */
  def unregisterCluster(cluster: String): Unit =
    catalog.unregisterCluster(cluster)

  /** Return table metadata */
  def getTableMetadata(tableIdent : TableIdent) : Option[TableMetaData] =
    catalog.getTableMetadata(tableIdent)

  /** Set an option of table options*/
  def setTableOption(tableIdent : TableIdent, key: String, value: String) : Unit =
    catalog.setTableOption(tableIdent, key, value)

  /** Remove an option from table options */
  def removeTableOption(tableIdent : TableIdent, key: String) : Unit =
    catalog.removeTableOption(tableIdent, key)

  /** Set table schema */
  def setTableSchema(tableIdent : TableIdent, schemaJsonString: String) : Unit =
    catalog.setTableSchema(tableIdent, schemaJsonString)

  /** Remove table schema */
  def removeTableSchema(tableIdent : TableIdent) : Unit =
    catalog.removeTableSchema(tableIdent)
}

object CassandraSQLContext {
  val CassandraSQLKeyspaceNameProperty = "spark.cassandra.keyspace"

  val Properties = Seq(
    CassandraSQLKeyspaceNameProperty
  )
}
