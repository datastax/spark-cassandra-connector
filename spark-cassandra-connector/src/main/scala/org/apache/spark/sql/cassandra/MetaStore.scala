package org.apache.spark.sql.cassandra

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{ResolvedDataSource, LogicalRelation}
import org.apache.spark.sql.types.{StructType, DataType}

import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.spark.connector.cql.{CassandraConnectorConf, Schema, CassandraConnector}

/**
 * Only registered table's metadata is stored in metastore's Cassandra table.
 * Unregistered Cassandra table's logical plan is constructed directly from
 * Cassandra table's schema.
 *
 * A cluster has databases/keyspaces. Keyspace is equivalent to database.
 * A database has tables.
 */
trait MetaStore {

  /**
   * Get a table's logical plan. If it's not found in metastore's Cassandra
   * table, then construct it directly from Cassandra table's schema.
   */
  def getTable(tableRef: TableRef) : LogicalPlan

  /** Get a registered table's metadata. Return None if the table is not found 
   * in metastore's Cassandra table.
   */
  def getRegisteredTableMetaData(tableRef: TableRef) : Option[TableMetaData]

  /** 
   * Get a registered table's logical plan from metastore's Cassandra table.
   * Return None if it's not found.
   */
  def getRegisteredTable(tableRef: TableRef) : Option[LogicalPlan]

  /**
   * Get all table names for a keyspace. If keyspace is None,
   * get all tables from all keyspaces.
   */
  def getTables(
      keyspace: Option[String],
      cluster: Option[String] = None) : Seq[(String, Boolean)]


  /** Return all database names */
  def getDatabases(cluster: Option[String] = None) : Seq[String]

  /** Return all cluster names */
  def getClusters() : Seq[String]

  /** Store a registered table's metadata in metastore's Cassandra table */
  def saveTable(
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
  def createDatabase(database: String, cluster: Option[String]) : Unit

  /** create a cluster in metastore */
  def createCluster(cluster: String) : Unit

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

class DataSourceMetaStore(sqlContext: SQLContext) extends MetaStore with Logging {

  import DataSourceMetaStore._
  import CassandraSourceRelation._
  import CassandraSQLContext._
  import DefaultSource._

  /** Connector to Metastore's Cassandra table */
  private val metaStoreConn = getCassandraConnector(metaStoreCluster)

  private val createMetaStoreKeyspaceQuery =
    s"""
      |CREATE KEYSPACE IF NOT EXISTS ${metaStoreKeyspace}
      | WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """.stripMargin.replaceAll("\n", " ")

  private val createMetaStoreTableQuery =
    s"""
      |CREATE TABLE IF NOT EXISTS ${metaStoreTableFullName}
      | (cluster_name text,
      |  keyspace_name text,
      |  table_name text,
      |  source_provider text,
      |  schema_json text,
      |  options map<text, text>,
      |  PRIMARY KEY (cluster_name, keyspace_name, table_name))
    """.stripMargin.replaceAll("\n", " ")

  private val insertTableQuery =
    s"""
      |INSERT INTO ${metaStoreTableFullName}
      | (schema_json, source_provider, options,
      |  cluster_name, keyspace_name, table_name)
      | values (?, ?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val insertTableWithoutSchemaQuery =
    s"""
      |INSERT INTO ${metaStoreTableFullName}
      | (source_provider, options, cluster_name, keyspace_name, table_name)
      | values (?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val insertTableNamesOnlyQuery =
    s"""
      |INSERT INTO ${metaStoreTableFullName}
      | (cluster_name, keyspace_name, table_name)
      | values (?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val deleteTableSchemaQuery =
    s"""
      |DELETE schema_json
      |FROM ${metaStoreTableFullName}
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val deleteTableQuery =
    s"""
      |DELETE FROM ${metaStoreTableFullName}
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val updateTableOptionQuery =
    s"""
      |UPDATE ${metaStoreTableFullName}
      |SET options[?] = ?
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val updateSchemaQuery =
    s"""
      |UPDATE ${metaStoreTableFullName}
      |SET schema_json = ?
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val deleteTableOptionQuery =
    s"""
      |DELETE options[?]
      |FROM ${metaStoreTableFullName}
      |WHERE cluster_name = ?
      | AND keyspace_name = ?
      | AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val selectTableMetaDataQuery =
    s"""
      |SELECT source_provider, schema_json, options
      |FROM ${metaStoreTableFullName}
      |WHERE cluster_name = ?
      |  AND keyspace_name = ?
      |  AND table_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val deleteClusterQuery =
    s"""
      |DELETE FROM ${metaStoreTableFullName}
      |WHERE cluster_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val selectTableKeyspaceQuery =
    s"""
      |SELECT table_name, keyspace_name
      |From ${metaStoreTableFullName}
      |WHERE cluster_name = ?
    """.stripMargin.replaceAll("\n", " ")

  private val selectClusterNamesQuery =
    s"""
      |SELECT cluster_name
      |From ${metaStoreTableFullName}
    """.stripMargin.replaceAll("\n", " ")

  private val selectKeyspaceNamesQuery =
    s"""
      |SELECT keyspace_name
      |From ${metaStoreTableFullName}
      |WHERE cluster_name = ?
    """.stripMargin.replaceAll("\n", " ")

  override def getTable(tableRef: TableRef): LogicalPlan = {
      getRegisteredTable(tableRef).getOrElse(
        getTableFromCassandraSchema(tableRef))
  }

  override def getTables(
      keyspace: Option[String],
      cluster: Option[String] = None): Seq[(String, Boolean)] = {
    val clusterName = cluster.getOrElse(getCluster)
    val names = ListBuffer[String]()
    // Add source tables from metastore
    val result = execute(selectTableKeyspaceQuery, clusterName).iterator()
    while (result.hasNext) {
      val row: Row = result.next()
      val tableName = row.getString(0)
      val ks = row.getString(1)
      if (tableName != TempDBOrTableName) {
        if (keyspace.nonEmpty && ks == keyspace.get ||
          keyspace.isEmpty) {
          names += tableName
        }
      }
    }

    // Add source tables from Cassandra tables
    val conn = getCassandraConnector(clusterName)
    if (keyspace.nonEmpty) {
      val schema = Schema.fromCassandra(conn)
      val ksDef = schema.keyspaceByName.get(keyspace.get)
      names ++= ksDef.map(_.tableByName.keySet).getOrElse(Set.empty)
    }
    names.distinct.toList.map(name => (name, false))
  }

  override def getDatabases(
      cluster: Option[String] = None): Seq[String] = {
    val registeredDatabaseNames =
      getRegisteredDatabases(cluster).toSet
    val clusterName = cluster.getOrElse(getCluster)
    // Add source tables from Cassandra tables
    val conn = getCassandraConnector(clusterName)
    val keyspacesFromCassandra =
      Schema.fromCassandra(conn).keyspaceByName.keySet

    (registeredDatabaseNames ++
      keyspacesFromCassandra --
      SystemKeyspaces).toSeq
  }

  def getRegisteredDatabases(
      cluster: Option[String] = None): Seq[String] = {
    val clusterName = cluster.getOrElse(getCluster)
    val names = ListBuffer[String]()
    // Add source tables from metastore
    val result =
      execute(selectKeyspaceNamesQuery, clusterName).iterator()
    while (result.hasNext) {
      val row: Row = result.next()
      val keyspaceName = row.getString(0)
      if (keyspaceName != TempDBOrTableName) {
        names += keyspaceName
      }
    }
    names.toList
  }

  override def getClusters(): Seq[String] = {
    (getRegisteredClusters ++ Seq(getCluster)).distinct
  }

  /** Get cluster names for registered tables from metastore's Cassandra table */
  private def getRegisteredClusters: Seq[String] = {
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

  /** Save a registered tale's metadata */
  override def saveTable(
      tableRef: TableRef,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    import collection.JavaConversions._

    val cluster = tableRef.cluster.getOrElse(getCluster)
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    if (schema.nonEmpty) {
      execute(
        insertTableQuery,
        schema.get.json,
        source,
        mapAsJavaMap(options),
        cluster,
        keyspace,
        table)
    } else {
      execute(
        insertTableWithoutSchemaQuery,
        source,
        mapAsJavaMap(options),
        cluster,
        keyspace,
        table)
    }

    // Remove temporary database or table
    val tempTableIdent = TableRef(
      TempDBOrTableName,
      TempDBOrTableName,
      Option(cluster))
    removeTable(tempTableIdent)
  }

  override def setTableOption(
      tableRef: TableRef,
      key: String,
      value: String) : Unit = {
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(
      updateTableOptionQuery,
      key,
      value,
      cluster,
      keyspace,
      table)
  }

  override def removeTableOption(
      tableRef: TableRef,
      key: String) : Unit = {
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(
      deleteTableOptionQuery,
      key,
      cluster,
      keyspace,
      table)
  }

  /** Set table schema */
  override def setTableSchema(
      tableRef: TableRef,
      schemaJsonString: String) : Unit = {
    try {
      DataType.fromJson(schemaJsonString)
    } catch {
      case _: Exception =>
        throw new RuntimeException(
          s"$schemaJsonString is in wrong schema json string format")
    }
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(
      updateSchemaQuery,
      schemaJsonString,
      cluster,
      keyspace,
      table)
  }

  /** Remove table schema */
  override def removeTableSchema(tableRef: TableRef) : Unit = {
    val cluster = tableRef.cluster.get
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    execute(deleteTableSchemaQuery, cluster, keyspace, table)
  }

  override def createDatabase(
      database: String,
      cluster: Option[String]) : Unit = {
    val databaseNames = getRegisteredDatabases(cluster).toSet
    if (!databaseNames.contains(database)) {
      val clusterName = cluster.getOrElse(getCluster)
      execute(
        insertTableNamesOnlyQuery,
        clusterName,
        database,
        TempDBOrTableName)
    }
  }

  override def createCluster(cluster: String) : Unit = {
    val clusterNames = getRegisteredClusters.toSet
    if (!clusterNames.contains(cluster)) {
      execute(
        insertTableNamesOnlyQuery,
        cluster,
        TempDBOrTableName,
        TempDBOrTableName)
    }
  }

  override def removeTable(tableRef: TableRef) : Unit = {
    val clusterName = tableRef.cluster.getOrElse(getCluster)
    execute(
      deleteTableQuery,
      clusterName,
      tableRef.keyspace,
      tableRef.table)
  }

  override def removeDatabase(
      database: String,
      cluster: Option[String]) : Unit = {
    val clusterName = cluster.getOrElse(getCluster)
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
      session => session.execute(s"TRUNCATE ${metaStoreTableFullName}")
    }
  }

  override def init() : Unit = {
    metaStoreConn.withSessionDo {
      session =>
        session.execute(createMetaStoreKeyspaceQuery)
        session.execute(createMetaStoreTableQuery)
    }
  }

  /**
   * Construct a logical plan for a registered table.
   * Return None if it's not found.
   */
  def getRegisteredTable(tableRef: TableRef): Option[LogicalPlan] = {
    val metadata = getRegisteredTableMetaData(tableRef)
    if (metadata.nonEmpty) {
      val data = metadata.get
      val relation = ResolvedDataSource(
        sqlContext,
        data.schema,
        data.source,
        data.options)
      Option(LogicalRelation(relation.relation))
    } else {
      None
    }
  }

  override def getRegisteredTableMetaData(
    tableRef: TableRef) : Option[TableMetaData] = {
    val clusterName = tableRef.cluster.getOrElse(getCluster)
    val keyspace = tableRef.keyspace
    val table = tableRef.table
    val result =
      execute(selectTableMetaDataQuery, clusterName, keyspace, table)
    if (result.isExhausted()) {
      None
    } else {
      val row: Row = result.one()
      val options : java.util.Map[String, String] =
        row.getMap("options", classOf[String], classOf[String])
      val schemaColumnValue = row.getString("schema_json")
      val schemaJsonString =
        Option(schemaColumnValue).getOrElse(
          options.get(CassandraDataSourceUserDefinedSchemaNameProperty))
      val schema : Option[StructType] =
        Option(schemaJsonString)
          .map(DataType.fromJson)
          .map(_.asInstanceOf[StructType])
      val source = row.getString("source_provider")

      // convert to scala Map
      import scala.collection.JavaConversions._
      Option(TableMetaData(tableRef, source, options.toMap, schema))
    }
  }

  /**
   * Construct a relation directly from Cassandra table's schema.
   * It may throw NoSuchTableException if it's not found.
   */
  private def getTableFromCassandraSchema(
      tableRef: TableRef) : LogicalPlan = {
    existInCassandra(tableRef)
    val sourceRelation = CassandraSourceRelation(tableRef, sqlContext)
    LogicalRelation(sourceRelation)
  }

  /** Check whether table is in Cassandra */
  private def existInCassandra(tableRef: TableRef) : Unit = {
    val clusterName = tableRef.cluster.getOrElse(getCluster)
    val conn = getCassandraConnector(clusterName)
    //Throw NoSuchTableException if table is not found in C*
    try {
      val schema = Schema.fromCassandra(conn)
      val keyspace = schema.keyspaceByName(tableRef.keyspace)
      keyspace.tableByName(tableRef.table)
    } catch {
      case _:NoSuchElementException => throw new NoSuchTableException
    }
  }

  /** Create a Cassandra connector for a cluster */
  private def getCassandraConnector(cluster: String) : CassandraConnector = {
    val sparkConf = sqlContext.sparkContext.getConf
    val sqlConf = sqlContext.getAllConfs
    val conf = sparkConf.clone()
    for (prop <- CassandraConnectorConf.Properties) {
      val clusterLevelValue = sqlConf.get(s"$cluster/$prop")
      if (clusterLevelValue.nonEmpty)
        conf.set(prop, clusterLevelValue.get)
    }
    new CassandraConnector(CassandraConnectorConf(conf))
  }

  /** Get current used metastore's cluster name */
  private def getCluster : String = {
    sqlContext.conf.getConf(
      CassandraSqlClusterNameProperty,
      defaultClusterName)
  }
  /** Get metastore Cassandra table's keyspace name */
  private def metaStoreKeyspace : String = {
    sqlContext.conf.getConf(
      CassandraDataSourceMetaStoreKeyspaceNameProperty,
      DefaultCassandraDataSourceMetaStoreKeyspaceName)
  }

  /** Get metastore's Cassandra table name */
  private def metaStoreTable : String = {
    sqlContext.conf.getConf(
      CassandraDataSourceMetaStoreTableNameProperty,
      DefaultCassandraDataSourceMetaStoreTableName)
  }

  /** Get metastore Cassandra table name with keyspace name */
  private def metaStoreTableFullName : String = {
    s"${metaStoreKeyspace}.${metaStoreTable}"
  }

  /** Get cluster name where metastore's Cassandra table resides */
  private def metaStoreCluster : String = {
    sqlContext.conf.getConf(
      CassandraDataSourceMetaStoreClusterNameProperty,
      defaultClusterName)
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

  private def execute(
    query: String,
    cluster: String,
    keyspace: String,
    table: String) : ResultSet = {
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
      session =>
        session.execute(query, key, value, cluster, keyspace, table)
    }
  }

  private def execute(
      query: String,
      value: String,
      cluster: String,
      keyspace: String,
      table: String) : ResultSet = {
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
      session =>
        session.execute(
          query,
          source,
          options,
          cluster,
          keyspace,
          table)
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
      session =>
        session.execute(
          query,
          schemaJson,
          source,
          options,
          cluster,
          keyspace,
          table)
    }
  }
}

object DataSourceMetaStore {
  val DefaultCassandraDataSourceMetaStoreKeyspaceName = "data_source_meta_store"
  val DefaultCassandraDataSourceMetaStoreTableName = "data_source_meta_store"

  val CassandraDataSourceMetaStoreClusterNameProperty =
    "spark.cassandra.datasource.metastore.cluster";
  val CassandraDataSourceMetaStoreKeyspaceNameProperty =
    "spark.cassandra.datasource.metastore.keyspace";
  val CassandraDataSourceMetaStoreTableNameProperty =
    "spark.cassandra.datasource.metastore.table";

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

  // This temporary entry in metastore's Cassandra table should be deleted once
  // there are real table for the cluster is saved into metastore. It shouldn't
  // return to client when list tables or databases.
  val TempDBOrTableName = "TO_BE_DELETED"
}

/** Table metadata */
case class TableMetaData(
      tableRef: TableRef,
      source : String,
      options: Map[String, String],
      schema: Option[StructType] = None)