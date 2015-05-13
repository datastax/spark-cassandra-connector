package org.apache.spark.sql.cassandra

import java.io.IOException
import java.util.concurrent.ExecutionException

import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, Catalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.types.StructType

import CassandraDefaultSource._

private[cassandra] class CassandraCatalog(cc: CassandraSQLContext) extends Catalog with Logging {

  override val caseSensitive: Boolean = true
  val metaStore: MetaStore = new DataSourceMetaStore(cc)

  // Create metastore keyspace and table if they don't exist
  metaStore.init()

  /** A cache of Spark SQL data source tables that have been accessed. Cache is thread safe.*/
  private[cassandra] val cachedDataSourceTables: LoadingCache[Seq[String], LogicalPlan] = {
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

  /** Obtain the Relation for a Cassandra table */
  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    val fullTableIdentifier = fullTableIdentifierFrom(tableIdentifier)
    val relation = cachedDataSourceTables.get(fullTableIdentifier)
    alias.map(a => Subquery(a, relation)).getOrElse(Subquery(tableName, relation))
  }

  /**
   * Only register table to local cache. To register table in metastore, use
   * registerTable(tableRef, source, schema, options) method
   */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    val fullTableIdentifier = fullTableIdentifierFrom(tableIdentifier)
    cachedDataSourceTables.put(fullTableIdentifier, plan)
  }

  /** Register a customized table metadata to metastore */
  def registerTable(
      tableRef: TableRef,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {

    val fullOptions =
      if (CassandraDefaultSource.cassandraDatasource(source)) {
        cc.optionsWithTableRef(tableRef, options)
      } else {
        options
      }
    synchronized {
      metaStore.storeTable(tableRef, source, schema, fullOptions)
    }
  }

  /** Unregister a table from local cache and metastore. */
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val fullTableIdentifier = fullTableIdentifierFrom(tableIdentifier)
    cachedDataSourceTables.invalidate(fullTableIdentifier)
    synchronized {
      metaStore.removeTable(tableRefFrom(tableIdentifier))
    }
  }

  /** Unregister table from local cache and metastore. */
  def unregisterTable(tableRef: TableRef): Unit = {
    unregisterTable(catalystTableIdentFrom(tableRef))
  }

  /** Unregister database from local cache and metastore. */
  def unregisterDatabase(database: String, cluster: Option[String]): Unit = {
    unregisterDatabaseFromCache(database, cluster)
    synchronized {
      metaStore.removeDatabase(database, cluster)
    }
  }

  /** Remove tables of given database from the local cache */
  private def unregisterDatabaseFromCache(database: String, cluster: Option[String]): Unit = {
    val tables = getTables(Option(database), cluster)
    val dbAndCluster = if (cluster.nonEmpty) Seq(cluster.get, database) else Seq(database)
    for (table <- tables) {
      val tableIdentifier = dbAndCluster ++ Seq(table)
      cachedDataSourceTables.invalidate(tableIdentifier)
    }
  }
  /** Unregister cluster from local cache and metastore. */
  def unregisterCluster(cluster: String): Unit = {
    val databases = getDatabases(Option(cluster))
    for (database <- databases) {
      unregisterDatabaseFromCache(database, Option(cluster))
    }
    synchronized {
      metaStore.removeCluster(cluster)
    }
  }

  /** Unregister all tables from local cache and metastore. */
  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
    synchronized {
      metaStore.removeAllTables()
    }
  }

  /** Check whether table exists */
  override def tableExists(tableIdentifier: Seq[String]): Boolean = synchronized {
    val fullTableIdentifier = fullTableIdentifierFrom(tableIdentifier)
    try {
      return cachedDataSourceTables.get(fullTableIdentifier) != null
    } catch {
      case _: ExecutionException =>
    }
    false
  }

  /** Check whether table exists */
  def tableExists(tableRef: TableRef): Boolean = synchronized {
    tableExists(catalystTableIdentFrom(tableRef))
  }

  /** Check whether table is stored in metastore */
  def tableExistsInMetastore(tableRef: TableRef): Boolean = synchronized {
    metaStore.getTableFromMetastore(tableRef).nonEmpty
  }

  /** All tables are not temporary tables */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    getTables(databaseName, None)
  }

  /** Get all tables for given database name and cluster */
  def getTables(databaseName: Option[String], cluster: Option[String] = None): Seq[(String, Boolean)] = synchronized {
    metaStore.getAllTables(databaseName, cluster)
  }

  /** Get all tables for given database name and cluster */
  def getDatabases(cluster: Option[String] = None): Seq[String] = synchronized {
    metaStore.getAllDatabases(cluster)
  }

  /** Get all tables for given database name and cluster */
  def getClusters(): Seq[String] = synchronized {
    metaStore.getAllClusters()
  }

  /** Create a database in metastore */
  def createDatabase(database: String, cluster: Option[String]): Unit = synchronized {
    metaStore.storeDatabase(database, cluster)
  }

  /** Create a cluster in metastore */
  def createCluster(cluster: String): Unit = synchronized {
    metaStore.storeCluster(cluster)
  }

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  def refreshTable(tableRef: TableRef): Unit = {
    cc.refreshCassandraSchema(tableRef.cluster.getOrElse(cc.getCluster))
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  override def refreshTable(databaseName: String, tableName: String): Unit = {
    refreshTable(TableRef(tableName, databaseName, Option(cc.getCluster)))
  }

  /** Return table metadata */
  def getTableMetadata(tableRef : TableRef) : Option[TableMetaData] = synchronized {
    metaStore.getTableMetaData(tableRef)
  }

  /** Set table schema */
  def setTableSchema(tableRef : TableRef, schemaJsonString: String) : Unit = {
    synchronized {
      metaStore.setTableSchema(tableRef, schemaJsonString)
    }
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableRef))
  }

  /** Set an option of table options*/
  def setTableOption(tableRef : TableRef, key: String, value: String) : Unit = {
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
    Seq(tableRef.cluster.getOrElse(cc.getCluster), tableRef.keyspace, tableRef.table)

  private[this] def fullTableIdentifierFrom(tableIdentifier: Seq[String]) : Seq[String] = {
    catalystTableIdentFrom(tableRefFrom(tableIdentifier))
  }
}

object CassandraCatalog {
  val CassandraSQLSourceProviderDisableProperty = "spark.cassandra.sql.sources.disable"
}