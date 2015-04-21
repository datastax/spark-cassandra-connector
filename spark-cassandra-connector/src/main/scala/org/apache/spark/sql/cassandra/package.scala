package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

import com.datastax.spark.connector.cql.{CassandraConnector, Schema, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf

package object cassandra {


  implicit def toRDDOfSQLRow[T <: RDD[CassandraSQLRow]](rdd: T): RDD[Row] = rdd.asInstanceOf[RDD[Row]]

  /**
   * Add a method, `cassandraTable`, to SQLContext that allows reading data stored in Cassandra.
   * Add methods to access per cluster level, keyspace level and table level Cassandra configuration
   * settings.
   */
  implicit class CSQLContext(sqlContext: SQLContext) {

    import CSQLContext._


    private val schemas = CacheBuilder.newBuilder
      .maximumSize(100)
      .expireAfterWrite(sqlContext.sparkContext.getConf.getLong(CassandraSchemaExpireInMinutesProperty,
      DefaultCassandraSchemaExpireInMinutes), TimeUnit.MINUTES)
      .build(
        new CacheLoader[String, Schema] {
          def load(cluster: String) : Schema = {
            val clusterOpt = if(DefaultCassandraClusterName.eq(cluster)) None else Option(cluster)
            Schema.fromCassandra(new CassandraConnector(sqlContext.getCassandraConnConf(clusterOpt)))
          }
        })

    /**
     * Create a DataFrame for a given Cassandra table of a keyspace and optional cluster, user defined schema
     * and scan type including [[PrunedFilteredScanType]], [[BaseScanType]], [[PrunedScanType]]
     * and [[CatalystScanType]]
     * */
    def cassandraTable(
        table: String,
        keyspace: String,
        scanType: ScanType = PrunedFilteredScanType,
        userSpecifiedSchema: Option[StructType] = None,
        cluster: Option[String] = None)(
        implicit connector: CassandraConnector = new CassandraConnector(sqlContext.getCassandraConnConf(cluster)),
        readConf: ReadConf = sqlContext.getReadConf(keyspace, table, cluster),
        writeConf: WriteConf = sqlContext.getWriteConf(keyspace, table, cluster)): DataFrame = {

      sqlContext.baseRelationToDataFrame(
        getDataSourceRelation(
          table,
          keyspace,
          scanType,
          userSpecifiedSchema,
          cluster)(connector, readConf, writeConf))

    }

    /** Get data source relation for given table, keyspace, scanType, cluster and userSpecifiedSchema */
    def getDataSourceRelation(
        table: String,
        keyspace: String,
        scanType: ScanType = PrunedFilteredScanType,
        userSpecifiedSchema: Option[StructType] = None,
        cluster: Option[String] = None)(
        implicit connector: CassandraConnector = new CassandraConnector(sqlContext.getCassandraConnConf(cluster)),
        readConf: ReadConf = sqlContext.getReadConf(keyspace, table, cluster),
        writeConf: WriteConf = sqlContext.getWriteConf(keyspace, table, cluster)) : BaseRelationImpl = {

        scanType.makeRelation(
          table,
          keyspace,
          cluster,
          userSpecifiedSchema,
          connector,
          readConf,
          writeConf,
          sqlContext)
    }

    /** Add table level read configuration settings. Set cluster to None for a single cluster */
    def addTableReadConf(
        table: String,
        keyspace: String,
        cluster: Option[String],
        conf: ReadConf): Unit = {

      readConfSettings.addTableConf(table, keyspace, cluster, conf)
    }


    /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelReadConf(
        keyspace: String,
        cluster: Option[String],
        conf: ReadConf): Unit = {

      readConfSettings.addKeyspaceLevelConf(keyspace, cluster, conf)
    }

    /** Add cluster level read configuration settings */
    def addClusterLevelReadConf(
        cluster: String,
        conf: ReadConf): Unit = {

      readConfSettings.addClusterLevelConf(cluster, conf)
    }

    /** Remove table level read configuration settings */
    def removeTableLevelReadConf(
        table: String,
        keyspace: String,
        cluster: Option[String]): Unit = {

      readConfSettings.removeTableLevelConf(table, keyspace, cluster)
    }

    /** Remove keyspace level read configuration settings */
    def removeKeyspaceLevelReadConf(
        keyspace: String,
        cluster: Option[String]): Unit = {

      readConfSettings.removeKeyspaceLevelConf(keyspace, cluster)
    }

    /** Remove cluster level read configuration settings */
    def removeClusterLevelReadConf(
        cluster: String): Unit = {

      readConfSettings.removeClusterLevelConf(cluster)
    }

    /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getReadConf(
        table: String,
        keyspace: String,
        cluster: Option[String]): ReadConf = {

      readConfSettings.getConf(table, keyspace, cluster, ReadConf.fromSparkConf(sqlContext.sparkContext.getConf))
    }

    /** Add table level write configuration settings. Set cluster to None for a single cluster */
    def addTableWriteConf(
        table: String,
        keyspace: String,
        cluster: Option[String],
        conf: WriteConf): Unit = {

      writeConfSettings.addTableConf(table, keyspace, cluster, conf)
    }

    /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelWriteConf(
        keyspace: String,
        writeConf: WriteConf,
        cluster: Option[String]) : Unit= {

      writeConfSettings.addKeyspaceLevelConf(keyspace, cluster, writeConf)
    }

    /** Add cluster level write configuration settings */
    def addClusterLevelWriteConf(
        cluster: String,
        conf: WriteConf) : Unit = {

      writeConfSettings.addClusterLevelConf(cluster, conf)
    }

    /** Remove table level write configuration settings */
    def removeTableLevelWriteConf(
        table: String,
        keyspace: String,
        cluster: Option[String]) : Unit = {

      writeConfSettings.removeTableLevelConf(table, keyspace, cluster)
    }

    /** Remove keyspace level write configuration settings */
    def removeKeyspaceLevelWriteConf(
        keyspace: String,
        cluster: Option[String]) : Unit = {

      writeConfSettings.removeKeyspaceLevelConf(keyspace, cluster)
    }

    /** Remove cluster level write configuration settings */
    def removeClusterLevelWriteConf(
        cluster: String) : Unit = {

      writeConfSettings.removeClusterLevelConf(cluster)
    }

    /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getWriteConf(
        table: String,
        keyspace: String,
        cluster: Option[String]): WriteConf = {

      writeConfSettings.getConf(table, keyspace, cluster, WriteConf.fromSparkConf(sqlContext.sparkContext.getConf))
    }

    /** Add cluster level write configuration settings */
    def addCassandraConnConf(
        cluster: String,
        conf: CassandraConnectorConf) : Unit = {

      connConfSettings.addClusterLevelConf(cluster, conf)
    }

    /** Remove cluster level write configuration settings */
    def removeClusterLevelCassandraConnConf(
        cluster: String) : Unit = {

      connConfSettings.removeClusterLevelConf(cluster)
    }

    /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
    def getCassandraConnConf(
        cluster: Option[String]) : CassandraConnectorConf = {

      connConfSettings.getConf(cluster, CassandraConnectorConf(sqlContext.sparkContext.getConf))
    }

    def getCassandraSchema(
        cluster: String): Schema = {

      schemas.get(cluster)
    }
  }

  object CSQLContext {
    val CassandraSchemaExpireInMinutesProperty = "spark.cassandra.schema.expire.in.minutes"
    val DefaultCassandraSchemaExpireInMinutes = 10
    val DefaultCassandraClusterName = "default"

    val Properties = Seq(
      CassandraSchemaExpireInMinutesProperty
    )

    /**
     * [[CassandraConfSetting]] stores per cluster level, per keyspace level and per table level
     * Cassandra configuration settings
     */
    val readConfSettings = new CassandraConfSetting[ReadConf]()
    val writeConfSettings = new CassandraConfSetting[WriteConf]()
    val connConfSettings = new CassandraConnConfSetting()

  }
}