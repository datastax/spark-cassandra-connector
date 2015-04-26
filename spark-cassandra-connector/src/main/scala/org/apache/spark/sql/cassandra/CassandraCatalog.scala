package org.apache.spark.sql.cassandra

import java.io.IOException

import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.sources.LogicalRelation

import com.datastax.spark.connector.cql.Schema
import CSQLContext._

private[cassandra] class CassandraCatalog(cc: CassandraSQLContext) extends Catalog with Logging {

  val caseSensitive: Boolean = true

  /** A cache of Spark SQL data source tables that have been accessed. */
  private[cassandra] val cachedDataSourceTables: LoadingCache[Seq[String], LogicalPlan] = {
    val cacheLoader = new CacheLoader[Seq[String], LogicalPlan]() {
      override def load(tableIdent: Seq[String]): LogicalPlan = {
        logDebug(s"Creating new cached data source for $tableIdent")
        val (cluster, database, table) = getClusterDBTableNames(tableIdent)
        val tableIdentifier = TableIdent(table, database, Option(cluster))
        val cassandraSourceRelation =
          cc.createCassandraSourceRelation(tableIdentifier, CassandraDataSourceOptions())
        LogicalRelation(cassandraSourceRelation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  /** Obtain the Relation for a Cassandra table */
  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    // Use source provider by default.
    val useSourceProvider = {
      val disable : String = cc.getConf(CassandraCatalog.CassandraSQLSourceProviderDisableProperty, "false")
      "false" == disable.trim.toLowerCase
    }

    if (useSourceProvider) {
      val relation = cachedDataSourceTables.get(tableIdentifier)
      alias.map(a => Subquery(a, relation)).getOrElse(Subquery(table, relation))
    } else {
      val schema = cc.getCassandraSchema(cluster)
      val keyspaceDef = schema.keyspaceByName.getOrElse(database, throw new IOException(s"Keyspace not found: $database"))
      val tableDef = keyspaceDef.tableByName.getOrElse(table, throw new IOException(s"Table not found: $database.$table"))
      val clusterOpt = if(DefaultCassandraClusterName.eq(cluster)) None else Option(cluster)
      val relation = CassandraRelation(tableDef, alias, clusterOpt)(cc)
      alias.map(a => Subquery(a, relation)).getOrElse(Subquery(table, relation))
    }
  }

  /** Retrieve table, keyspace and cluster names from table identifier. Use default name if it's not found */
  private def getClusterDBTableNames(tableIdentifier: Seq[String]): (String, String, String) = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val clusterName = id(2).getOrElse(DefaultCassandraClusterName)
    val keyspaceName = id(1).getOrElse(cc.getKeyspace)
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    (clusterName, keyspaceName, tableName)
  }

  /** Only register table to local cache. */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    cachedDataSourceTables.put(tableIdentifier, plan)
  }

  /** Only unregister table from local cache. */
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    cachedDataSourceTables.invalidate(tableIdentifier)
  }

  /** Only unregister all tables from local cache. */
  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }

  /** Check whether table exists in Cassandra */
  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    val schema = cc.getCassandraSchema(cluster)
    val tableDef =
      for (ksDef <- schema.keyspaceByName.get(database);
           tableDef <- ksDef.tableByName.get(table)) yield tableDef
    tableDef.isDefined
  }

  /** All tables are not temporary tables */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val schema = cc.getCassandraSchema(DefaultCassandraClusterName)
    if (databaseName.nonEmpty) {
      getTables(databaseName.get, schema)
    } else {
      var result = Seq[(String, Boolean)]()
      for (ksDef <- schema.keyspaces) {
        result ++= getTables(ksDef.keyspaceName, schema)
      }
      result
    }
  }

  /** Return table names and all are not temporary tables */
  private def getTables(databaseName: String, schema: Schema): Seq[(String, Boolean)] = {
    val ksDef = schema.keyspaceByName(databaseName)
    ksDef.tableByName.keySet.map(name => (name, false)).toSeq
  }

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  override def refreshTable(databaseName: String, tableName: String): Unit = {
    cc.refreshCassandraSchema(DefaultCassandraClusterName)
    cachedDataSourceTables.refresh(Seq(DefaultCassandraClusterName, databaseName, tableName))
  }
}

object CassandraCatalog {
  val CassandraSQLSourceProviderDisableProperty = "spark.cassandra.sql.sources.disable"
}