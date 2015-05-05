package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraDefaultSource._
import org.apache.spark.sql.sources.PrunedScan

import com.datastax.spark.connector.cql.{CassandraConnector, Schema, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf

package object cassandra {


  /** Converts [[RDD[CassandraSQLRow]]]] to [[[RDD[Row]]]] */
  implicit def toRDDOfSQLRow[T <: RDD[CassandraSQLRow]](rdd: T): RDD[Row] = rdd.asInstanceOf[RDD[Row]]

  /**
   * Add a method, `cassandraTable`, to SQLContext that allows reading from and writing to Cassandra.
   * Add methods to access cluster level, keyspace level and table level Cassandra configuration settings.
   */
  implicit class CSQLContext(sqlContext: SQLContext) {

    import CSQLContext._

    /** local cache of Cassandra schema metadata */
    private val schemas = CacheBuilder.newBuilder
      .maximumSize(100)
      .expireAfterWrite(sqlContext.sparkContext.getConf
      .getLong(CassandraSchemaExpireInMinutesProperty, DefaultCassandraSchemaExpireInMinutes), TimeUnit.MINUTES)
      .build(
        new CacheLoader[String, Schema] {
          def load(cluster: String) : Schema = {
            Schema.fromCassandra(new CassandraConnector(sqlContext.getCassandraConnConf(cluster)))
          }
        })

    /**
     * Create a DataFrame for a given Cassandra table given tableIdent, pushDown and user defined schema
     * parameters. If pushDown is disable, [[PrunedScan]] is used.
     */
    def cassandraTable(
        tableIdent: TableIdent,
        sourceOptions: CassandraDataSourceOptions = CassandraDataSourceOptions())(
      implicit
        connector: CassandraConnector = new CassandraConnector(sqlContext.getCassandraConnConf(
          tableIdent.cluster.getOrElse(sqlContext.getCluster))),
        readConf: ReadConf = sqlContext.getReadConf(tableIdent),
        writeConf: WriteConf = sqlContext.getWriteConf(tableIdent)) : DataFrame = {

      sqlContext.baseRelationToDataFrame(
        CassandraSourceRelation(tableIdent, sqlContext)(
          connector = connector,
          readConf = readConf,
          writeConf = writeConf,
          sourceOptions = sourceOptions))
    }

    /**
     * Create a Relation for a given Cassandra table given table, keyspace, cluster, pushDown and user defined schema
     * parameters. If pushDown is disable, [[PrunedScan]] is used.
     */
    def createCassandraSourceRelation(
        tableIdent: TableIdent,
        sourceOptions: CassandraDataSourceOptions = CassandraDataSourceOptions())(
      implicit
        connector: CassandraConnector = new CassandraConnector(sqlContext.getCassandraConnConf(
          tableIdent.cluster.getOrElse(sqlContext.getCluster))),
        readConf: ReadConf = sqlContext.getReadConf(tableIdent),
        writeConf: WriteConf = sqlContext.getWriteConf(tableIdent)): BaseRelationImpl = {

      CassandraSourceRelation(tableIdent, sqlContext)(
        connector = connector,
        readConf = readConf,
        writeConf = writeConf,
        sourceOptions = sourceOptions)
    }

    /** Add table level read configuration settings. Set cluster to None for a single cluster */
    def addTableReadConf(conf: ReadConf, tableIdent: TableIdent): Unit = {
      readConfSettings.addTableConf(tableIdentWithCluster(tableIdent), conf)
    }

    /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelReadConf(
        conf: ReadConf,
        keyspace: String,
        cluster: String = getCluster): Unit = {
      readConfSettings.addKeyspaceLevelConf(keyspace, cluster, conf)
    }

    /** Add cluster level read configuration settings */
    def addClusterLevelReadConf(conf: ReadConf, cluster: String = getCluster): Unit = {
      readConfSettings.addClusterLevelConf(cluster, conf)
    }

    /** Remove table level read configuration settings */
    def removeTableLevelReadConf(tableIdent: TableIdent): Unit = {
      readConfSettings.removeTableLevelConf(tableIdentWithCluster(tableIdent))
    }

    /** Remove keyspace level read configuration settings */
    def removeKeyspaceLevelReadConf(keyspace: String, cluster: String = getCluster): Unit = {
      readConfSettings.removeKeyspaceLevelConf(keyspace, cluster)
    }

    /** Remove cluster level read configuration settings */
    def removeClusterLevelReadConf(cluster: String = getCluster): Unit = {
      readConfSettings.removeClusterLevelConf(cluster)
    }

    /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getReadConf(tableIdent: TableIdent): ReadConf = {
      readConfSettings.getConf(
        tableIdentWithCluster(tableIdent),
        ReadConf.fromSparkConf(sqlContext.sparkContext.getConf))
    }

    /** Add table level write configuration settings. Set cluster to None for a single cluster */
    def addTableWriteConf(conf: WriteConf, tableIdent: TableIdent): Unit = {
      writeConfSettings.addTableConf(tableIdentWithCluster(tableIdent), conf)
    }

    /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelWriteConf(
        conf: WriteConf,
        keyspace: String,
        cluster: String = getCluster): Unit = {
      writeConfSettings.addKeyspaceLevelConf(keyspace, cluster, conf)
    }

    /** Add cluster level write configuration settings */
    def addClusterLevelWriteConf(conf: WriteConf, cluster: String = getCluster) : Unit = {
      writeConfSettings.addClusterLevelConf(cluster, conf)
    }

    /** Remove table level write configuration settings */
    def removeTableLevelWriteConf(tableIdent: TableIdent) : Unit = {
      writeConfSettings.removeTableLevelConf(tableIdentWithCluster(tableIdent))
    }

    /** Remove keyspace level write configuration settings */
    def removeKeyspaceLevelWriteConf(keyspace: String, cluster: String = getCluster) : Unit = {
      writeConfSettings.removeKeyspaceLevelConf(keyspace, cluster)
    }

    /** Remove cluster level write configuration settings */
    def removeClusterLevelWriteConf(cluster: String = getCluster) : Unit = {
      writeConfSettings.removeClusterLevelConf(cluster)
    }

    /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getWriteConf(tableIdent: TableIdent): WriteConf = {
      writeConfSettings.getConf(
        tableIdentWithCluster(tableIdent),
        WriteConf.fromSparkConf(sqlContext.sparkContext.getConf))
    }

    /** Add cluster level write configuration settings */
    def addCassandraConnConf(conf: CassandraConnectorConf, cluster: String = getCluster) : Unit = {
      connConfSettings.addClusterLevelConf(cluster, conf)
    }

    /** Remove cluster level write configuration settings */
    def removeClusterLevelCassandraConnConf(cluster: String = getCluster) : Unit = {
      connConfSettings.removeClusterLevelConf(cluster)
    }

    /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
    def getCassandraConnConf(cluster: String = getCluster) : CassandraConnectorConf = {
      connConfSettings.getClusterLevelConf(cluster, CassandraConnectorConf(sqlContext.sparkContext.getConf))
    }

    /** Return Cassandra schema metadata for a cluster */
    def getCassandraSchema(cluster: String): Schema = {
      schemas.get(cluster)
    }

    /** Return Cassandra schema metadata for a cluster */
    def refreshCassandraSchema(cluster: String) : Unit = {
      schemas.invalidate(cluster)
    }

    /** Return cluster name */
    def getCluster : String = {
      sqlContext.getConf(CassandraClusterNameProperty, DefaultCassandraClusterName)
    }

    /** Return database name */
    def getDatabase : String = {
      sqlContext.getConf(CassandraDatabaseNameProperty)
    }

    /** Set current used database name */
    def useDatabase(database: String) = {
      sqlContext.setConf(CassandraDatabaseNameProperty, database)
    }

    /** Set current used database name */
    def useCluster(cluster: String) = {
      sqlContext.setConf(CassandraClusterNameProperty, cluster)
    }

    /** Add table names to options */
    def optionsWithTableIdent(tableIdent: TableIdent, options: Map[String, String]) : Map[String, String] = {
      Map[String, String](
        CassandraDataSourceClusterNameProperty -> tableIdent.cluster.getOrElse(getCluster),
        CassandraDataSourceKeyspaceNameProperty -> tableIdent.keyspace,
        CassandraDataSourceTableNameProperty -> tableIdent.table
      ) ++ options
    }

    private def tableIdentWithCluster(tableIdent: TableIdent) : TableIdent = {
      if (tableIdent.cluster.nonEmpty)
        tableIdent
      else
        TableIdent(tableIdent.table, tableIdent.keyspace, Option(getCluster))
    }
  }

  object CSQLContext {
    val CassandraSchemaExpireInMinutesProperty = "spark.cassandra.schema.expire.in.minutes"
    val CassandraDatabaseNameProperty = "spark.cassandra.sql.database"
    val CassandraClusterNameProperty = "spark.cassandra.sql.cluster"
    val DefaultCassandraSchemaExpireInMinutes = 10
    val DefaultCassandraClusterName = "default"

    val Properties = Seq(
      CassandraSchemaExpireInMinutesProperty,
      CassandraClusterNameProperty,
      CassandraDatabaseNameProperty
    )

    /** Stores per cluster level, keyspace level and table level Cassandra configuration settings */
    val readConfSettings = new CassandraConfSettings[ReadConf]()
    val writeConfSettings = new CassandraConfSettings[WriteConf]()
    val connConfSettings = new CassandraClusterLevelConfSettings[CassandraConnectorConf]()

    /** Quote name */
    def quoted(str: String): String = {
      "\"" + str + "\""
    }
  }
}