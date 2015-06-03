package org.apache.spark.sql.cassandra

import scala.collection.mutable
import java.io.IOException

import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector, Schema}
import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.sources.LogicalRelation

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
    cachedDataSourceTables.put(tableIdentifier, plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    cachedDataSourceTables.invalidate(tableIdentifier)
  }

  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val schema = Schema.fromCassandra(getCassandraConnector(cluster))
    val tabDef =
      for (ksDef <- schema.keyspaceByName.get(database);
           tabDef <- ksDef.tableByName.get(table)) yield tabDef
    tabDef.nonEmpty
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val cluster = csc.getCluster
    val schema = Schema.fromCassandra(getCassandraConnector(cluster))
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

  private def getCassandraConnector(cluster: String) : CassandraConnector = {
    val conf = csc.sparkContext.getConf.clone()
    val sqlConf = csc.getAllConfs
    for (prop <- CassandraConnectorConf.Properties) {
      val clusterLevelValue = sqlConf.get(s"$cluster/$prop")
      if (clusterLevelValue.nonEmpty)
        conf.set(prop, clusterLevelValue.get)
    }
    new CassandraConnector(CassandraConnectorConf(conf))
  }

  def refreshTable(databaseName: String, tableName: String): Unit = {
    cachedDataSourceTables.refresh(Seq(csc.getCluster, databaseName, tableName))
  }
}
