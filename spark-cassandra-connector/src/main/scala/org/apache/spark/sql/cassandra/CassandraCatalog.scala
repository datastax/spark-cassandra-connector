package org.apache.spark.sql.cassandra

import scala.collection.mutable
import java.io.IOException

import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector, Schema}
import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.sources.LogicalRelation

import CassandraSourceRelation._

import scala.collection.mutable.ListBuffer

private[cassandra] class CassandraCatalog(csc: CassandraSQLContext) extends Catalog with Logging {

  val caseSensitive: Boolean = true

  /** A cache of Spark SQL data source tables that have been accessed. Cache is thread safe.*/
  private[cassandra] val cachedDataSourceTables: LoadingCache[Seq[String], LogicalPlan] = {
    val cacheLoader = new CacheLoader[Seq[String], LogicalPlan]() {
      override def load(tableIdent: Seq[String]): LogicalPlan = {
        logDebug(s"Creating new cached data source for ${tableIdent.mkString(".")}")
        buildRelation(tableIdent)
      }
    }
    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val tableIdent = fullTableIdent(tableIdentifier)
    val tableLogicPlan = cachedDataSourceTables.get(tableIdent)
    alias.map(a => Subquery(a, tableLogicPlan)).getOrElse(tableLogicPlan)
  }

  /** Build logic plan from a CassandraSourceRelation */
  private def buildRelation(tableIdentifier: Seq[String]): LogicalPlan = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val tableRef = TableRef(table, database, Option(cluster))
    val sourceRelation = CassandraSourceRelation(tableRef, csc, CassandraSourceOptions())
    Subquery(table, LogicalRelation(sourceRelation))
  }

  /** Return cluster, database and table names from a table identifier*/
  private def getClusterDBTableNames(tableIdentifier: Seq[String]): (String, String, String) = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val cluster = id(2).getOrElse(csc.getCluster)
    val database = id(1).getOrElse(csc.getKeyspace)
    val table = id(0).getOrElse(throw new IOException(s"Missing table name"))
    (cluster, database, table)
  }

  /** Return a table identifier with table name, keyspace name and cluster name */
  private def fullTableIdent(tableIdentifier: Seq[String]) : Seq[String] = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    Seq(cluster, database, table)
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    val tableIdent = fullTableIdent(tableIdentifier)
    cachedDataSourceTables.put(tableIdent, plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val tableIdent = fullTableIdent(tableIdentifier)
    cachedDataSourceTables.invalidate(tableIdent)
  }

  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val tableRef = TableRef(table, database, Option(cluster))
    val tableIdent = fullTableIdent(tableIdentifier)
    val cached = cachedDataSourceTables.asMap().containsKey(tableIdent)
    if (cached) {
      true
    } else {
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
    val schema = Schema.fromCassandra(getCassandraConnector(tableRef))
    if (databaseName.nonEmpty) {
      val ksDef = schema.keyspaceByName.get(databaseName.get)
      if (ksDef.nonEmpty) {
        ksDef.get.tables.map(
          tableDef => (s"${ksDef.get.keyspaceName}.${tableDef.tableName}", false)).toSeq
      } else {
        Seq.empty
      }
    } else {
      val tables = mutable.Set[(String, Boolean)]()
      for (ksDef <- schema.keyspaces) {
        tables ++= ksDef.tables.map(
          table => (s"${ksDef.keyspaceName}.${table.tableName}", false))
      }
      tables.toSeq
    }
  }

  /** List all tables for a given database name and cluster from local cache */
  def getTablesFromCache(
      databaseName: Option[String],
      cluster: Option[String] = None): Seq[(String, Boolean)] = {
    val iter = cachedDataSourceTables.asMap().keySet().iterator()
    val clusterName = cluster.getOrElse(csc.getCluster)
    val names = ListBuffer[String]()
    while (iter.hasNext) {
      val ident = iter.next()
      val c = ident(0)
      val db = ident(1)
      val table = ident(2)
      if (clusterName == c) {
        if (databaseName.nonEmpty && databaseName.get == db) {
          names += table
        } else if (databaseName.isEmpty) {
          names += s"$db.$table"
        }
      }
    }
    names.toList.map((_, true))
  }

  private def getCassandraConnector(tableRef: TableRef) : CassandraConnector = {
    val sparkConf = csc.sparkContext.getConf.clone()
    val sqlConf = csc.getAllConfs
    val conf = consolidateConfs(sparkConf, sqlConf, tableRef, Map.empty)
    new CassandraConnector(CassandraConnectorConf(conf))
  }

  def refreshTable(databaseName: String, tableName: String): Unit = {
    cachedDataSourceTables.refresh(Seq(csc.getCluster, databaseName, tableName))
  }
}
