package org.apache.spark.sql.cassandra

import java.util.concurrent.ExecutionException

import org.apache.spark.sql.cassandra.DefaultSource._
import org.apache.spark.sql.types.StructType

import java.io.IOException

import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}

private[cassandra] class CassandraCatalog(
      cc: CassandraSQLContext)
      extends Catalog
      with Logging {

  val caseSensitive: Boolean = true
  val metaStore: MetaStore = new DataSourceMetaStore(cc)

  // Create metastore keyspace and table if they don't exist
  metaStore.init()

  /**
   * A cache of Spark SQL data source tables that have been accessed.
   * Cache is thread safe.
   */
  private[cassandra] val cachedDataSourceTables:
    LoadingCache[Seq[String], LogicalPlan] = {
    val cacheLoader = new CacheLoader[Seq[String], LogicalPlan]() {
      override def load(tableIdent: Seq[String]): LogicalPlan = {
        logDebug(s"Creating new cached data source for ${tableIdent.mkString(".")}")
        synchronized {
          metaStore.getTable(tableRefFrom(tableIdent))
        }
      }
    }
    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = fullTableIdent(tableIdentifier)
    val tableLogicPlan = cachedDataSourceTables.get(tableIdent)
    alias.map(a => Subquery(a, tableLogicPlan)).getOrElse(tableLogicPlan)
  }

  /** Return cluster, database and table names from a table identifier*/
  private def getClusterDBTableNames(
      tableIdentifier: Seq[String]): (String, String, String) = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val cluster = id(2).getOrElse(cc.getCluster)
    val database = id(1).getOrElse(cc.getKeyspace)
    val table = id(0).getOrElse(throw new IOException(s"Missing table name"))
    (cluster, database, table)
  }

  /** Return a table identifier with table name, keyspace name and cluster name */
  private def fullTableIdent(
      tableIdentifier: Seq[String]) : Seq[String] = {
    val (cluster, database, table) = getClusterDBTableNames(tableIdentifier)
    Seq(cluster, database, table)
  }

  /**
   * Only register table to local cache. To register table
   * in metastore, use registerTable(tableRef, source, schema, options) method
   */
  override def registerTable(
      tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    val tableIdent = fullTableIdent(tableIdentifier)
    cachedDataSourceTables.put(tableIdent, plan)
  }

  /** Register a table metadata to metastore */
  def registerTable(
      tableRef: TableRef,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    val fullOptions =
      if (DefaultSource.cassandraSource(source)) {
        optionsWithTableRef(tableRef, options)
      } else {
        options
      }
    synchronized {
      metaStore.saveTable(tableRef, source, schema, fullOptions)
    }
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val tableIdent = fullTableIdent(tableIdentifier)
    cachedDataSourceTables.invalidate(tableIdent)
    synchronized {
      metaStore.removeTable(tableRefFrom(tableIdent))
    }
  }

  /** Remove table from local cache and metastore. */
  def unregisterTable(tableRef: TableRef): Unit = {
    val tableIdent = Seq(
      tableRef.cluster.getOrElse(cc.getCluster),
      tableRef.keyspace,
      tableRef.table)
    cachedDataSourceTables.invalidate(tableIdent)
    synchronized {
      metaStore.removeTable(tableRef)
    }
  }

  /** Unregister a database from local cache and metastore. */
  def unregisterDatabase(
      database: String,
      cluster: Option[String]): Unit = {
    unregisterDatabaseFromCache(database, cluster)
    synchronized {
      metaStore.removeDatabase(database, cluster)
    }
  }

  /** Remove tables of given database from the local cache */
  private def unregisterDatabaseFromCache(
      database: String,
      cluster: Option[String]): Unit = {
    val tables = getTables(Option(database), cluster)
    val dbAndCluster = {
      if (cluster.nonEmpty)
        Seq(cluster.get, database)
      else
        Seq(database)
    }
    for (table <- tables) {
      val tableIdentifier = dbAndCluster ++ Seq(table)
      cachedDataSourceTables.invalidate(tableIdentifier)
    }
  }

  /** Unregister a cluster from local cache and metastore. */
  def unregisterCluster(cluster: String): Unit = {
    val databases = getDatabases(Option(cluster))
    for (database <- databases) {
      unregisterDatabaseFromCache(database, Option(cluster))
    }
    synchronized {
      metaStore.removeCluster(cluster)
    }
  }

  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
    synchronized {
      metaStore.removeAllTables()
    }
  }

  /** Check whether table exists */
  override def tableExists(
      tableIdentifier: Seq[String]): Boolean = synchronized {
    val fullTableIdentifier = fullTableIdentFrom(tableIdentifier)
    try {
      return cachedDataSourceTables.get(fullTableIdentifier) != null
    } catch {
      case e: ExecutionException =>
    }
    false
  }

  /** Check whether table exists */
  def tableExists(tableRef: TableRef): Boolean = synchronized {
    tableExists(catalystTableIdentFrom(tableRef))
  }

  /** Check whether table is stored in metastore table */
  def tableExistsInMetastore(tableRef: TableRef): Boolean = synchronized {
    metaStore.getRegisteredTable(tableRef).nonEmpty
  }

  /** List all tables */
  // TODO: May return DataFrame
  override def getTables(
      databaseName: Option[String]): Seq[(String, Boolean)] = {
    getTables(databaseName, None)
  }

  /** List all tables for a given database name and cluster */
  // TODO: May return DataFrame
  def getTables(
     databaseName: Option[String],
     cluster: Option[String] = None): Seq[(String, Boolean)] = synchronized {
    metaStore.getTables(databaseName, cluster)
  }

  /** List all databases for a given cluster */
  // TODO: May return DataFrame
  def getDatabases(
      cluster: Option[String] = None): Seq[String] = synchronized {
    metaStore.getDatabases(cluster)
  }

  /** List all clusters */
  // TODO: May return DataFrame
  def getClusters(): Seq[String] = synchronized {
    metaStore.getClusters()
  }

  /** Create a database in metastore */
  def createDatabase(
      database: String,
      cluster: Option[String]): Unit = synchronized {
    metaStore.createDatabase(database, cluster)
  }

  /** Create a cluster in metastore */
  def createCluster(cluster: String): Unit = synchronized {
    metaStore.createCluster(cluster)
  }

  /**
   * Refresh CassandraContext schema cache,
   * then refresh table in local cache
   */
  def refreshTable(tableRef: TableRef): Unit = {
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Refresh table in local cache */
  override def refreshTable(
      databaseName: String,
      tableName: String): Unit = {
    refreshTable(TableRef(tableName, databaseName, Option(cc.getCluster)))
  }

  /** Return table metadata */
  def getTableMetadata(
      tableRef : TableRef) : Option[TableMetaData] = synchronized {
    metaStore.getRegisteredTableMetaData(tableRef)
  }

  /** Set table schema */
  def setTableSchema(
      tableRef : TableRef,
      schemaJsonString: String) : Unit = {
    synchronized {
      metaStore.setTableSchema(tableRef, schemaJsonString)
    }
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Set an option of table options*/
  def setTableOption(
      tableRef : TableRef,
      key: String,
      value: String) : Unit = {
    synchronized {
      metaStore.setTableOption(tableRef, key, value)
    }
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Remove an option from table options */
  def removeTableOption(tableRef : TableRef, key: String) : Unit = {
    synchronized {
      metaStore.removeTableOption(tableRef, key)
    }
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Remove table schema */
  def removeTableSchema(tableRef : TableRef) : Unit = {
    synchronized {
      metaStore.removeTableSchema(tableRef)
    }
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Convert Catalyst tableIdentifier to TableRef */
  def tableRefFrom(tableIdentifier: Seq[String]) : TableRef = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val clusterName = id(2).getOrElse(cc.getCluster).trim
    val keyspaceName = id(1).getOrElse(cc.getKeyspace).trim
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    TableRef(tableName.trim, keyspaceName, Option(clusterName))
  }

  /** Convert TableRef to Catalyst tableIdentifier */
  def catalystTableIdentFrom(tableRef: TableRef) : Seq[String] =
    Seq(
      tableRef.cluster.getOrElse(cc.getCluster),
      tableRef.keyspace,
      tableRef.table)

  private[this] def fullTableIdentFrom(
      tableIdentifier: Seq[String]) : Seq[String] = {
    catalystTableIdentFrom(tableRefFrom(tableIdentifier))
  }

  /** Add table names to options */
  def optionsWithTableRef(
      tableRef: TableRef,
      options: Map[String, String]) : Map[String, String] = {

    Map[String, String](
      CassandraDataSourceClusterNameProperty ->
        tableRef.cluster.getOrElse(cc.getCluster),
      CassandraDataSourceKeyspaceNameProperty -> tableRef.keyspace,
      CassandraDataSourceTableNameProperty -> tableRef.table
    ) ++ options
  }
}
