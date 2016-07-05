package org.apache.spark.sql.cassandra

import scala.util.Try

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.{CatalystConf, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, Schema}

private[cassandra] class CassandraCatalog(
    cs: CassandraSession,
    fnResourceLoader: FunctionResourceLoader,
    fnRegistry: FunctionRegistry,
    conf: CatalystConf,
    hadoopConf: Configuration
) extends SessionCatalog(cs.externalCatalog, fnResourceLoader, fnRegistry, conf, hadoopConf) {

  private val csc = cs.wrapped match {
    case sqlCtx: CassandraSQLContext => sqlCtx
    case sqlCtx: SQLContext =>
      val ctx = new CassandraSQLContext(cs)
      cs.setWrappedContext(ctx)
      ctx
  }

  val caseSensitive: Boolean = true

  /** A cache of Spark SQL data source tables that have been accessed. Cache is thread safe.*/
  private[cassandra] val cachedDataSourceTables: LoadingCache[TableIdentifier, LogicalPlan] = {
    val cacheLoader = new CacheLoader[TableIdentifier, LogicalPlan]() {
      override def load(tableIdent: TableIdentifier): LogicalPlan = {
        logDebug(s"Creating new cached data source for $tableIdent")
        buildRelation(tableIdent)
      }
    }
    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def lookupRelation(tableIdent: TableIdentifier, alias: Option[String]): LogicalPlan = {
    Try(cachedDataSourceTables.get(tableIdent))
        .map(plan => alias.map(a => SubqueryAlias(a, plan)).getOrElse(plan))
        .getOrElse(super.lookupRelation(tableIdent, alias))
  }

  /** Build logic plan from a CassandraSourceRelation */
  private def buildRelation(tableIdentifier: TableIdentifier): LogicalPlan = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val tableRef = TableRef(table, database, Option(cluster))
    val sourceRelation = CassandraSourceRelation(tableRef, csc, CassandraSourceOptions())
    SubqueryAlias(table, LogicalRelation(sourceRelation))
  }

  /** Return cluster, database and table names from a table identifier*/
  private def getClusterDBTableNames(tableIdent: TableIdentifier): (String, String, String) = {
    val database = tableIdent.database.getOrElse(csc.getKeyspace)
    val table = tableIdent.table
    (csc.getCluster, database, table)
  }

  override def databaseExists(db: String): Boolean = {
    val cluster = csc.getCluster
    val tableRef = TableRef("", db, Option(cluster))
    val schema = Schema.fromCassandra(getCassandraConnector(tableRef), Some(db))
    schema.keyspaces.nonEmpty || super.databaseExists(db)
  }

  override def tableExists(tableIdent: TableIdentifier): Boolean = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdent)
    val cached = cachedDataSourceTables.asMap().containsKey(tableIdent)
    if (cached) {
      true
    } else {
      val tableRef = TableRef(table, database, Option(cluster))
      val schema = Schema.fromCassandra(getCassandraConnector(tableRef))
      val tabDef =
        for (ksDef <- schema.keyspaceByName.get(database);
             tabDef <- ksDef.tableByName.get(table)) yield tabDef
      tabDef.nonEmpty
    }
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.refresh(tableIdent)
  }


  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    if (name.funcName.toLowerCase == "count") {
      return Count(children)
    }
    super.lookupFunction(name, children)
  }

  private def getCassandraConnector(tableRef: TableRef) : CassandraConnector = {
    val sparkConf = csc.sparkContext.getConf.clone()
    val sqlConf = csc.getAllConfs
    val conf = consolidateConfs(sparkConf, sqlConf, tableRef, Map.empty)
    new CassandraConnector(CassandraConnectorConf(conf))
  }
}
