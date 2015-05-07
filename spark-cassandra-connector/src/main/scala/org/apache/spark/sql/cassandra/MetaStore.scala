package org.apache.spark.sql.cassandra


import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{ResolvedDataSource, LogicalRelation}
import org.apache.spark.sql.types.{StructType, DataType}

import com.datastax.driver.core.{Row, PreparedStatement}
import com.datastax.spark.connector.cql.{Schema, CassandraConnector}


trait MetaStore {

  /**
   * Get a table from metastore. If it's not found in metastore, then look
   * up Cassandra tables to get the source table.
   *
   */
  def getTable(tableIdent: TableIdent) : LogicalPlan

  /** Get a table's metadata */
  def getTableMetaData(tableIdent: TableIdent) : Option[TableMetaData]

  /** Get a table from metastore. If it's not found in metastore, return None */
  def getTableFromMetastore(tableIdent: TableIdent) : Option[LogicalPlan]

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
      tableIdent: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]) : Unit

  /** Update table options */
  def setTableSchema(tableIdent: TableIdent, scheamJsonString: String) : Unit

  /** Update table options */
  def setTableOption(tableIdent: TableIdent, key: String, value: String) : Unit

  /** Remove an option from table options */
  def removeTableOption(tableIdent: TableIdent, key: String) : Unit

  /** Remove table schema from metadata */
  def removeTableSchema(tableIdent: TableIdent) : Unit

  /** create a database in metastore */
  def storeDatabase(database: String, cluster: Option[String]) : Unit

  /** create a cluster in metastore */
  def storeCluster(cluster: String) : Unit

  /** Remove table from metastore */
  def removeTable(tableIdent: TableIdent) : Unit

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
      | (cluster_name, keyspace_name, table_name, source_provider, schema_json, options)
      | values (?, ?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

  private val InsertIntoMetaStoreWithoutSchemaQuery =
    s"""
      |INSERT INTO ${getMetaStoreTableFullName}
      | (cluster_name, keyspace_name, table_name, source_provider, options)
      | values (?, ?, ?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")


  override def getTable(tableIdent: TableIdent): LogicalPlan = {
      getTableFromMetastore(tableIdent).getOrElse(getTableMayThrowException(tableIdent))
  }

  override def getAllTables(keyspace: Option[String], cluster: Option[String] = None): Seq[(String, Boolean)] = {
    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    val selectQuery =
      s"""
      |SELECT table_name, keyspace_name
      |From ${getMetaStoreTableFullName}
      |WHERE cluster_name = '$clusterName'
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[(String, Boolean)]()
    // Add source tables from metastore
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val tableName = row.getString(0)
          if (tableName != TempDatabaseOrTableToBeDeletedName) {
            val ks = row.getString(1)
            if (keyspace.nonEmpty && ks == keyspace.get ||
              ks != TempDatabaseOrTableToBeDeletedName) {
                names += ((tableName, false))
            }
          }
        }
    }

    // Add source tables from Cassandra tables
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    if (keyspace.nonEmpty) {
      val ksDef = Schema.fromCassandra(conn).keyspaceByName.get(keyspace.get)
      names ++= ksDef.map(_.tableByName.keySet).getOrElse(Set.empty).map((name => (name, false)))
    }
     names.toList
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
    val selectQuery =
      s"""
      |SELECT keyspace_name
      |From ${getMetaStoreTableFullName}
      |WHERE cluster_name = '$clusterName'
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[String]()
    // Add source tables from metastore
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val keyspaceName = row.getString(0)
          if (keyspaceName != TempDatabaseOrTableToBeDeletedName) {
            names += keyspaceName
          }
        }
    }
    names.toList
  }

  override def getAllClusters(): Seq[String] = {
    (getAllClustersFromMetastore ++ Seq(sqlContext.getCluster)).distinct
  }

  def getAllClustersFromMetastore: Seq[String] = {
    val selectQuery =
      s"""
      |SELECT cluster_name
      |From ${getMetaStoreTableFullName}
    """.stripMargin.replaceAll("\n", " ")
    val names = ListBuffer[String]()
    // Add source tables from metastore
    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val clusterName = row.getString(0)
          names += clusterName
        }
    }

    names.toList
  }

  /** Store a tale with the creation meta data */
  override def storeTable(
      tableIdent: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {
    import collection.JavaConversions._

    val cluster = tableIdent.cluster.getOrElse(sqlContext.getCluster)
    if (schema.nonEmpty) {
      metaStoreConn.withSessionDo {
        session =>
          val preparedStatement = session.prepare(InsertIntoMetaStoreQuery)
          session.execute(
            preparedStatement.bind(
              cluster,
              tableIdent.keyspace,
              tableIdent.table,
              source,
              schema.get.json,
              mapAsJavaMap(options)))
      }
    } else {
      metaStoreConn.withSessionDo {
        session =>
          val preparedStatement: PreparedStatement = session.prepare(InsertIntoMetaStoreWithoutSchemaQuery)
          session.execute(
            preparedStatement.bind(
              cluster,
              tableIdent.keyspace,
              tableIdent.table,
              source,
              mapAsJavaMap(options)))
      }
    }

    // Remove temporary database or table
    val tempTableIdent = TableIdent(
      TempDatabaseOrTableToBeDeletedName,
      TempDatabaseOrTableToBeDeletedName,
      Option(cluster))
    removeTable(tempTableIdent)
  }

  override def setTableOption(tableIdent: TableIdent, key: String, value: String) : Unit = {
    val updateQuery =
      s"""
      |UPDATE ${getMetaStoreTableFullName}
      |SET options['$key'] = '$value'
      |WHERE cluster_name = '${tableIdent.cluster.get}'
      | AND keyspace_name = '${tableIdent.keyspace}'
      | AND table_name = '${tableIdent.table}'
    """.stripMargin.replaceAll("\n", " ")
    metaStoreConn.withSessionDo {
      session => session.execute(updateQuery)
    }
  }

  override def removeTableOption(tableIdent: TableIdent, key: String) : Unit = {
    val deleteQuery =
      s"""
      |DELETE options['$key']
      |FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = '${tableIdent.cluster.get}'
      | AND keyspace_name = '${tableIdent.keyspace}'
      | AND table_name = '${tableIdent.table}'
    """.stripMargin.replaceAll("\n", " ")
    metaStoreConn.withSessionDo {
      session => session.execute(deleteQuery)
    }
  }

  /** Set table schema */
  override def setTableSchema(tableIdent: TableIdent, scheamJsonString: String) : Unit = {
    try {
      DataType.fromJson(scheamJsonString)
    } catch {
      case e: Exception => throw new RuntimeException(s"$scheamJsonString is in wrong schema json string format")
    }

    val updateQuery =
        s"""
      |UPDATE ${getMetaStoreTableFullName}
      |SET schema_json = '$scheamJsonString'
      |WHERE cluster_name = '${tableIdent.cluster.get}'
      | AND keyspace_name = '${tableIdent.keyspace}'
      | AND table_name = '${tableIdent.table}'
    """.stripMargin.replaceAll("\n", " ")
    metaStoreConn.withSessionDo {
      session => session.execute(updateQuery)
    }
  }

  /** Remove table schema */
  override def removeTableSchema(tableIdent: TableIdent) : Unit = {
    val deleteQuery =
      s"""
      |DELETE schema_json
      |FROM ${getMetaStoreTableFullName}
      |WHERE cluster_name = '${tableIdent.cluster.get}'
      | AND keyspace_name = '${tableIdent.keyspace}'
      | AND table_name = '${tableIdent.table}'
    """.stripMargin.replaceAll("\n", " ")
    metaStoreConn.withSessionDo {
      session => session.execute(deleteQuery)
    }
  }
  override def storeDatabase(database: String, cluster: Option[String]) : Unit = {
    val databaseNames = getAllDatabasesFromMetastore(cluster).toSet
    if (databaseNames.contains(database))
      return

    val insertQuery =
      s"""
      |INSERT INTO ${getMetaStoreTableFullName}
      | (cluster_name, keyspace_name, table_name)
      | values (?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    metaStoreConn.withSessionDo {
      session =>
        val preparedStatement = session.prepare(insertQuery)
        session.execute(
          preparedStatement.bind(clusterName, database, TempDatabaseOrTableToBeDeletedName))
    }
  }

  override def storeCluster(cluster: String) : Unit = {
    val clusterNames = getAllClustersFromMetastore.toSet
    if (clusterNames.contains(cluster))
      return

    val insertQuery =
      s"""
      |INSERT INTO ${getMetaStoreTableFullName}
      | (cluster_name, keyspace_name, table_name)
      | values (?, ?, ?)
    """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session =>
        val preparedStatement = session.prepare(insertQuery)
        session.execute(
          preparedStatement.bind(
            cluster,
            TempDatabaseOrTableToBeDeletedName,
            TempDatabaseOrTableToBeDeletedName))
    }
  }

  override def removeTable(tableIdent: TableIdent) : Unit = {
    val deleteQuery =
      s"""
        |DELETE FROM ${getMetaStoreTableFullName}
        |WHERE cluster_name = '${tableIdent.cluster.getOrElse(sqlContext.getCluster)}'
        | AND keyspace_name = '${tableIdent.keyspace}'
        | AND table_name = '${tableIdent.table}'
      """.stripMargin.replaceAll("\n", " ")
    metaStoreConn.withSessionDo {
      session => session.execute(deleteQuery)
    }
  }

  override def removeDatabase(database: String, cluster: Option[String]) : Unit = {
    val clusterName = cluster.getOrElse(sqlContext.getCluster)
    val selectQuery =
      s"""
      |SELECT table_name, keyspace_name
      |From ${getMetaStoreTableFullName}
      |WHERE cluster_name = '$clusterName'
    """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery).iterator()
        while (result.hasNext) {
          val row: Row = result.next()
          val keyspaceName = row.getString(1)
          if (keyspaceName == database) {
            val tableName = row.getString(0)
            removeTable(TableIdent(tableName, keyspaceName, Option(clusterName)))
          }
        }
    }
  }

  override def removeCluster(cluster: String) : Unit = {
    val deleteQuery =
      s"""
        |DELETE FROM ${getMetaStoreTableFullName}
        |WHERE cluster_name = '$cluster'
      """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session => session.execute(deleteQuery)
    }
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
  def getTableFromMetastore(tableIdent: TableIdent): Option[LogicalPlan] = {
    val metadata = getTableMetaData(tableIdent)
    if (metadata.nonEmpty) {
      val data = metadata.get
      val relation = ResolvedDataSource(sqlContext, data.schema, data.source, data.options)
      Option(LogicalRelation(relation.relation))
    } else {
      None
    }
  }

  override def getTableMetaData(tableIdent: TableIdent) : Option[TableMetaData] = {
    val selectQuery =
      s"""
        |SELECT source_provider, schema_json, options
        |FROM ${getMetaStoreTableFullName}
        |WHERE cluster_name = '${tableIdent.cluster.getOrElse(sqlContext.getCluster)}'
        |  AND keyspace_name = '${tableIdent.keyspace}'
        |  AND table_name = '${tableIdent.table}'
      """.stripMargin.replaceAll("\n", " ")

    metaStoreConn.withSessionDo {
      session =>
        val result = session.execute(selectQuery)
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
          Option(TableMetaData(tableIdent, source, options.toMap, schema))
        }
    }
  }
  /** Create a Relation directly from Cassandra table. It may throw NoSuchTableException if it's not found. */
  private def getTableMayThrowException(tableIdent: TableIdent) : LogicalPlan = {
    existInCassandra(tableIdent)
    val sourceRelation = sqlContext.createCassandraSourceRelation(tableIdent, CassandraDataSourceOptions())
    LogicalRelation(sourceRelation)
  }

  /** Check whether table is in Cassandra */
  private def existInCassandra(tableIdent: TableIdent) : Unit = {
    val clusterName = tableIdent.cluster.getOrElse(sqlContext.getCluster)
    val conn = new CassandraConnector(sqlContext.getCassandraConnConf(clusterName))
    //Throw NoSuchElementException if can't find table in C*
    try {
      Schema.fromCassandra(conn).keyspaceByName(tableIdent.keyspace).tableByName(tableIdent.table)
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
    TableIdent: TableIdent,
    source : String,
    options: Map[String, String],
    schema: Option[StructType] = None)