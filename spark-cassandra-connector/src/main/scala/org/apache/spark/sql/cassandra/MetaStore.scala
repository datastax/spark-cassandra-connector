package org.apache.spark.sql.cassandra


import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{ResolvedDataSource, LogicalRelation}
import org.apache.spark.sql.types.{StructType, DataType}

import com.datastax.driver.core.{ResultSet, Row, PreparedStatement}
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}


trait MetaStore {

  /**
   * Get a table from metastore. If it's not found in metastore, then look
   * up Cassandra tables to get the source table.
   *
   */
  def getTable(tableRef: TableRef) : LogicalPlan

  /** Get a table's metadata */
  def getTableMetaData(tableRef: TableRef) : Option[TableMetaData]

  /** Get a table from metastore. If it's not found in metastore, return None */
  def getTableFromMetastore(tableRef: TableRef) : Option[LogicalPlan]

  /**
   * Get all table names for a keyspace. If keyspace is empty, get all tables from
   * all keyspaces.
   */
  def getAllTables(keyspace: Option[String], cluster: Option[String] = None) : Seq[(String, Boolean)]


  /** Return all database names */
  def getAllDatabases(cluster: Option[String] = None) : Seq[String]

  /** Return all cluster names */
  def getAllClusters() : Seq[String]

  /** Only Store customized tables meta data in metastore */
  def storeTable(
      tableRef: TableRef,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]) : Unit

  /** Update table Schema */
  def setTableSchema(tableRef: TableRef, schemaJsonString: String) : Unit

  /** Update table options */
  def setTableOption(tableRef: TableRef, key: String, value: String) : Unit

  /** Remove an option from table options */
  def removeTableOption(tableRef: TableRef, key: String) : Unit

  /** Remove table schema from metadata */
  def removeTableSchema(tableRef: TableRef) : Unit

  /** create a database in metastore */
  def storeDatabase(database: String, cluster: Option[String]) : Unit

  /** create a cluster in metastore */
  def storeCluster(cluster: String) : Unit

  /** Remove table from metastore */
  def removeTable(tableRef: TableRef) : Unit

  /** Remove a database from metastore */
  def removeDatabase(database: String, cluster: Option[String]) : Unit

  /** Remove a cluster from metastore */
  def removeCluster(cluster: String) : Unit

  /** Remove all tables from metastore */
  def removeAllTables() : Unit

  /** Create metastore keyspace and table in Cassandra */
  def init() : Unit

}

/**
 * Store only customized tables or other data source tables. Cassandra data source tables
 * are directly lookup from Cassandra tables
 */
class DataSourceMetaStore(sqlContext: SQLContext) extends MetaStore with Logging {

  import DataSourceMetaStore._
  import CassandraDefaultSource._

  private val metaStoreConn = new CassandraConnector(sqlContext.getCassandraConnConf(getMetaStoreCluster))

  private val CreateMetaStoreKeyspaceQuery =
    s"""
      |CREATE KEYSPACE IF NOT EXISTS ${getMetaStoreKeyspace}
      | WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """.stripMargin.replaceAll("\n", " ")

  private val CreateMetaStoreTableQuery =
    s"""
      |CREATE TABLE IF NOT EXISTS ${getMetaStoreTableFullName}
      | (cluster_name text,
      |  keyspace_name text,
      |  table_name text,
      |  source_provider text,
      |  schema_json text,
      |  options map<text, text>,
      |  PRIMARY KEY (cluster_name, keyspace_name, table_name))
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreQuery =
    s"""
      |INSERT INTO ${getMetaStoreTableFullName}
      | (schema_json, source_provider, options, cluster_name, keyspace_name, table_name)
      | values (?, ?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreWithoutSchemaQuery =
    s"""
      |INSERT INTO ${getMetaStoreTableFullName}
      | (source_provider, options, cluster_name, keyspace_name, table_name)
      | values (?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val insertTableQuery =
    s"""
      |INSERT INTO ${getMetaStoreTableFullName}
      | (cluster_name, keyspace_name, table_name)
      | values (?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val deleteTableSchemaQuery =
    s"""
      |DELETE schema_json
      |FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val deleteTableQuery =
    s"""
      |DELETE FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val updateTableOptionQuery =
    s"""
      |UPDATE ${getMetaStoreTableFullName}
      |SET options[?] = ?
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val updateSchemaQuery =
    s"""
      |UPDATE ${getMetaStoreTableFullName}
      |SET schema_json = ?
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val deleteTableOptionQuery =
    s"""
      |DELETE options[?]
      |FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val selectTableMetaDataQuery =
    s"""
      |SELECT source_provider, schema_json, options
      |FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
      |  AND keyspace_name = ?
      |  AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val deleteClusterQuery =
    s"""
      |DELETE FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val selectTableKeyspaceQuery =
    s"""
      |SELECT table_name, keyspace_name
      |From ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val selectClusterNamesQuery =
    s"""
      |SELECT cluster_name
      |From ${getMetaStoreTableFullName}
    """.stripMargin.replaceAll("\n", " ")

  private val selectKeyspaceNamesQuery =
    s"""
      |SELECT keyspace_name
      |From ${getMetaStoreTableFullName}
      |WHERE cluster_name = ?
    """.stripMargin.replaceAll("\n", " ")

  override def getTable(tableRef: TableRef): LogicalPlan = {
      getTableFromMetastore(tableRef).getOrElse(getTableMayThrowException(tableRef))
  }

  override def getAllTables(keyspace: Option[String], cluster: Option[String] = None): Seq[(String, Boolean)] = {
    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    val names = ListBuffer[String]()
    // Add source tables from metastore
    val result = execute(selectTableKeyspaceQuery, clusterName).iterator()
    while (result.hasNext) {
      val row: Row = result.next()
      val tableName = row.getString(0)
      if (tableName != TempDatabaseOrTableToBeDeletedName) {
        val ks = row.getString(1)
        if (keyspace.nonEmpty && ks == keyspace.get ||
          ks != TempDatabaseOrTableToBeDeletedName) {
          names += tableName
        }
      }
    }

    // Add source tables from Cassandra tables
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    if (keyspace.nonEmpty) {
      val ksDef = Schema.fromCassandra(conn).keyspaceByName.get(keyspace.get)
      names ++= ksDef.map(_.tableByName.keySet).getOrElse(Set.empty)
    }
    names.distinct.toList.map(name => (name, false))
  }

  override def getAllDatabases(cluster: Option[String] = None): Seq[String] = {
    val databaseNamesFromMetastore = getAllDatabasesFromMetastore(cluster).toSet
    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    // Add source tables from Cassandra tables
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    val keyspacesFromCassandra = Schema.fromCassandra(conn).keyspaceByName.keySet
    (databaseNamesFromMetastore ++ keyspacesFromCassandra -- SystemKeyspaces).toSeq
  }

  def getAllDatabasesFromMetastore(cluster: Option[String] = None): Seq[String] = {
    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    val names = ListBuffer[String]()
    // Add source tables from metastore
    val result = execute(selectKeyspaceNamesQuery, clusterName).iterator()
    while (result.hasNext) {
      val row: Row = result.next()
      val keyspaceName = row.getString(0)
      if (keyspaceName != TempDatabaseOrTableToBeDeletedName) {
        names += keyspaceName
      }
    }
    names.toList
  }

  override def getAllClusters(): Seq[String] = {
    (getAllClustersFromMetastore ++ Seq(sqlContext.getCluster)).distinct
  }

  def getAllClustersFromMetastore: Seq[String] = {
    val names = ListBuffer[String]()
    // Add source tables from metastore
    val result = execute(selectClusterNamesQuery).iterator()
    while (result.hasNext) {
      val row: Row = result.next()
      val clusterName = row.getString(0)
      names += clusterName
    }

    names.toList
  }

  /** Store a tale with the creation meta data */
  override def storeTable(
      tableRef: TableRef,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    import collection.JavaConversions._

    val cluster = tableRef.cluster.getOrElse(sqlContext.getCluster)
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    if (schema.nonEmpty) {
      execute(InsertIntoMetaStoreQuery, schema.get.json, source, mapAsJavaMap(options), cluster, keyspace, table)
    } else {
      execute(InsertIntoMetaStoreWithoutSchemaQuery, source, mapAsJavaMap(options), cluster, keyspace, table)
    }

    // Remove temporary database or table
    val tempTableIdent = TableRef(
      TempDatabaseOrTableToBeDeletedName,
      TempDatabaseOrTableToBeDeletedName,
      Option(cluster))
    removeTable(tempTableIdent)
  }

  override def setTableOption(tableRef: TableRef, key: String, value: String) : Unit = {
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(updateTableOptionQuery, key, value, cluster, keyspace, table)
  }

  override def removeTableOption(tableRef: TableRef, key: String) : Unit = {
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(deleteTableOptionQuery, key, cluster, keyspace, table)
  }

  /** Set table schema */
  override def setTableSchema(tableRef: TableRef, schemaJsonString: String) : Unit = {
    try {
      DataType.fromJson(schemaJsonString)
    } catch {
      case _: Exception => throw new RuntimeException(s"$schemaJsonString is in wrong schema json string format")
    }
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(updateSchemaQuery, schemaJsonString, cluster, keyspace, table)
  }

  /** Remove table schema */
  override def removeTableSchema(tableRef: TableRef) : Unit = {
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(deleteTableSchemaQuery, cluster, keyspace, table)
  }

  override def storeDatabase(database: String, cluster: Option[String]) : Unit = {
    val databaseNames = getAllDatabasesFromMetastore(cluster).toSet
    if (!databaseNames.contains(database)) {
      val clusterName = cluster.getOrElse(sqlContext.getCluster)
      execute(insertTableQuery, clusterName, database, TempDatabaseOrTableToBeDeletedName)
    }
  }

  override def storeCluster(cluster: String) : Unit = {
    val clusterNames = getAllClustersFromMetastore.toSet
    if (!clusterNames.contains(cluster)) {
      execute(insertTableQuery, cluster, TempDatabaseOrTableToBeDeletedName, TempDatabaseOrTableToBeDeletedName)
    }
  }

  override def removeTable(tableRef: TableRef) : Unit = {
    val clusterName = tableRef.cluster.getOrElse(sqlContext.getCluster)
    execute(deleteTableQuery, clusterName, tableRef.keyspace, tableRef.table)
  }

  override def removeDatabase(database: String, cluster: Option[String]) : Unit = {
    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    val result = execute(selectTableKeyspaceQuery, clusterName).iterator()
    while (result.hasNext) {
      val row: Row = result.next()
      val keyspaceName = row.getString(1)
      if (keyspaceName == database) {
        val tableName = row.getString(0)
        removeTable(TableRef(tableName, keyspaceName, Option(clusterName)))
      }
    }
  }

  override def removeCluster(cluster: String) : Unit = {
    execute(deleteClusterQuery, cluster)
  }

  override def removeAllTables() : Unit = {
    metaStoreConn.withSessionDo {
      session => session.execute(s"TRUNCATE ${getMetaStoreTableFullName}")
    }
  }

  override def init() : Unit = {
    metaStoreConn.withSessionDo {
      session =>
        session.execute(CreateMetaStoreKeyspaceQuery)
        session.execute(CreateMetaStoreTableQuery)
    }
  }

  /** Look up source table from metastore */
  def getTableFromMetastore(tableRef: TableRef): Option[LogicalPlan] = {
    val metadata = getTableMetaData(tableRef)
    if (metadata.nonEmpty) {
      val data = metadata.get
      val relation = ResolvedDataSource(sqlContext, data.schema, data.source, data.options)
      Option(LogicalRelation(relation.relation))
    } else {
      None
    }
  }

  override def getTableMetaData(tableRef: TableRef) : Option[TableMetaData] = {
    val clusterName = tableRef.cluster.getOrElse(sqlContext.getCluster)
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    val result = execute(selectTableMetaDataQuery, clusterName, keyspace, table)
    if (result.isExhausted()) {
      None
    } else {
      val row: Row = result.one()
      val options : java.util.Map[String, String] = row.getMap("options", classOf[String], classOf[String])
      val schemaColumnValue = row.getString("schema_json")
      val schemaJsonString =
        Option(schemaColumnValue).getOrElse(options.get(CassandraDataSourceUserDefinedSchemaNameProperty))
      val schema : Option[StructType] =
        Option(schemaJsonString).map(DataType.fromJson).map(_.asInstanceOf[StructType])
      val source = row.getString("source_provider")

      // convert to scala Map
      import scala.collection.JavaConversions._
      Option(TableMetaData(tableRef, source, options.toMap, schema))
    }
  }
  /** Create a Relation directly from Cassandra table. It may throw NoSuchTableException if it's not found. */
  private def getTableMayThrowException(tableRef: TableRef) : LogicalPlan = {
    existInCassandra(tableRef)
    val sourceRelation = sqlContext.createCassandraSourceRelation(tableRef, CassandraDataSourceOptions())
    LogicalRelation(sourceRelation)
  }

  /** Check whether table is in Cassandra */
  private def existInCassandra(tableRef: TableRef) : Unit = {
    val clusterName = tableRef.cluster.getOrElse(sqlContext.getCluster)
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    //Throw NoSuchElementException if can't find table in C*
    try {
      Schema.fromCassandra(conn).keyspaceByName(tableRef.keyspace).tableByName(tableRef.table)
    } catch {
      case _:NoSuchElementException => throw new NoSuchTableException
    }
  }

  /** Get metastore schema keyspace name */
  private def getMetaStoreKeyspace : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreKeyspaceNameProperty,
      DefaultCassandraDataSourceMetaStoreKeyspaceName)
  }

  /** Get metastore schema table name */
  private def getMetaStoreTable : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreTableNameProperty,
      DefaultCassandraDataSourceMetaStoreTableName)
  }

  /** Return metastore schema table name with keyspace */
  private def getMetaStoreTableFullName : String = {
    s"${getMetaStoreKeyspace}.${getMetaStoreTable}"
  }

  /** Get cluster name where metastore resides */
  private def getMetaStoreCluster : String = {
    sqlContext.conf.getConf(CassandraDataSourceMetaStoreClusterNameProperty, sqlContext.getCluster)
  }

  private def execute(query: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query)
    }
  }
  private def execute(query: String, cluster: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query, cluster)
    }
  }

  private def execute(query: String, cluster: String, keyspace: String, table: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query, cluster, keyspace, table)
    }
  }

  private def execute(
      query: String,
      key: String,
      value: String,
      cluster: String,
      keyspace: String,
      table: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query, key, value, cluster, keyspace, table)
    }
  }

  private def execute(query: String, value: String, cluster: String, keyspace: String, table: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query, value, cluster, keyspace, table)
    }
  }

  private def execute(
      query: String,
      source: String,
      options: java.util.Map[String, String],
      cluster: String,
      keyspace: String,
      table: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query, source, options, cluster, keyspace, table)
    }
  }

  private def execute(
      query: String,
      schemaJson: String,
      source: String,
      options: java.util.Map[String, String],
      cluster: String,
      keyspace: String,
      table: String) : ResultSet = {
    metaStoreConn.withSessionDo {
      session => session.execute(query, schemaJson, source, options, cluster, keyspace, table)
    }
  }

}

object DataSourceMetaStore {
  val DefaultCassandraDataSourceMetaStoreKeyspaceName = "data_source_meta_store"
  val DefaultCassandraDataSourceMetaStoreTableName = "data_source_meta_store"

  val CassandraDataSourceMetaStoreClusterNameProperty = "spark.cassandra.datasource.metastore.cluster";
  val CassandraDataSourceMetaStoreKeyspaceNameProperty = "spark.cassandra.datasource.metastore.keyspace";
  val CassandraDataSourceMetaStoreTableNameProperty = "spark.cassandra.datasource.metastore.table";

  val Properties = Seq(
    CassandraDataSourceMetaStoreClusterNameProperty,
    CassandraDataSourceMetaStoreKeyspaceNameProperty,
    CassandraDataSourceMetaStoreTableNameProperty
  )

  private val DSESystemKeyspace = "dse_system"
  private val DSESecurityKeyspace = "dse_security"
  private val HiveMetastoreKeyspace = "HiveMetaStore"
  private val CFSKeyspace = "cfs"
  private val CFSArchiveKeyspace = "cfs_archive"
  private val CassandraSystemKeyspace = "system"
  private val CassandraSystemTraceKeyspace = "system_traces"
  private val CassandraSystemAuthKeyspace = "system_auth"

  val SystemKeyspaces =
    Set(
      CassandraSystemKeyspace,
      CassandraSystemTraceKeyspace,
      CassandraSystemAuthKeyspace,
      DefaultCassandraDataSourceMetaStoreKeyspaceName,
      DSESystemKeyspace,
      DSESecurityKeyspace,
      HiveMetastoreKeyspace,
      CFSKeyspace,
      CFSArchiveKeyspace)

  // This temporary entry in metastore should be deleted once there are real table
  // for the cluster is inserted into metastore. It shouldn't return to client when
  // list tables or databases.
  val TempDatabaseOrTableToBeDeletedName = "TO_BE_DELETED"
}

/** Store table metadata */
case class TableMetaData(
    tableRef: TableRef,
    source : String,
    options: Map[String, String],
    schema: Option[StructType] = None)