package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.rdd.CassandraTableScanRDD

import scala.collection.JavaConversions._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.spark.Logging
import org.apache.spark.sql.cassandra.CassandraSourceRelation._
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.{CatalystConf, SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf, Schema}

private[cassandra] class CassandraCatalog(csc: CassandraSQLContext) extends Catalog with Logging {

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
    val tableLogicPlan = cachedDataSourceTables.get(tableIdent)
    alias.map(a => Subquery(a, tableLogicPlan)).getOrElse(tableLogicPlan)
  }

  /** Build logic plan from a CassandraSourceRelation */
  private def buildRelation(tableIdentifier: TableIdentifier): LogicalPlan = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val tableRef = TableRef(table, database, Option(cluster))
    val sourceRelation = CassandraSourceRelation(tableRef, csc, CassandraSourceOptions())
    Subquery(table, LogicalRelation(sourceRelation))
  }

  /** Return cluster, database and table names from a table identifier*/
  private def getClusterDBTableNames(tableIdent: TableIdentifier): (String, String, String) = {
    val database = tableIdent.database.getOrElse(csc.getKeyspace)
    val table = tableIdent.table
    (csc.getCluster, database, table)
  }

  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    cachedDataSourceTables.put(tableIdent, plan)
  }

  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.invalidate(tableIdent)
  }

  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
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

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val cluster = csc.getCluster
    val tableNamesFromCache = getTablesFromCache(databaseName, Option(cluster)).map(_._1)
    val tablesFromCassandra = getTablesFromCassandra(databaseName)
    val tablesOnlyInCache =
      tableNamesFromCache.diff(tablesFromCassandra.map(_._1)).map(name => (name, true))

    tablesFromCassandra ++ tablesOnlyInCache
  }

  /** List all tables for a given database name and cluster directly from Cassandra */
  def getTablesFromCassandra(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val cluster = csc.getCluster
    val tableRef = TableRef("", databaseName.getOrElse(""), Option(cluster))
    val schema = Schema.fromCassandra(getCassandraConnector(tableRef), databaseName)
    for {
      ksDef <- schema.keyspaces.toSeq
      tableDef <- ksDef.tables
    } yield (s"${ksDef.keyspaceName}.${tableDef.tableName}", false)
  }

  /** List all tables for a given database name and cluster from local cache */
  def getTablesFromCache(
      databaseName: Option[String],
      cluster: Option[String] = None): Seq[(String, Boolean)] = {

    val clusterName = cluster.getOrElse(csc.getCluster)
    for (Seq(c, db, table) <- cachedDataSourceTables.asMap().keySet().toSeq
         if c == clusterName && databaseName.forall(_ == db)) yield {
      (s"$db.$table", true)
    }
  }

  private def getCassandraConnector(tableRef: TableRef) : CassandraConnector = {
    val sparkConf = csc.sparkContext.getConf.clone()
    val sqlConf = csc.getAllConfs
    val conf = consolidateConfs(sparkConf, sqlConf, tableRef, Map.empty)
    new CassandraConnector(CassandraConnectorConf(conf))
  }

  override val conf: CatalystConf = SimpleCatalystConf(caseSensitive)

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.refresh(tableIdent)
  }
}
