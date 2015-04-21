package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import com.datastax.spark.connector.cql.{CassandraConnector, Schema, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

package object cassandra {


  implicit def toRDDOfSQLRow[T <: RDD[CassandraSQLRow]](rdd: T): RDD[Row] = rdd.asInstanceOf[RDD[Row]]

  /**
   * Adds a method, `cassandraTable`, to SQLContext that allows reading data stored in Cassandra.
   */
  implicit class CSQLContext(sqlContext: SQLContext) {
    import CSQLContext._
    /**
     * Create a DataFrame for a given Cassandra table of a keyspace and optional cluster, user defined schema
     * and scan type including [[PrunedFilteredScanType]], [[BaseScanType]], [[PrunedScanType]]
     * and [[CatalystScanType]]
     * */
    def cassandraTable(table: String,
                       keyspace: String,
                       scanType: ScanType = PrunedFilteredScanType,
                       cluster: Option[String] = None,
                       userSpecifiedSchema: Option[StructType] = None): DataFrame =
      sqlContext.baseRelationToDataFrame(getDataSourceRelation(table, keyspace, scanType, cluster, userSpecifiedSchema))

    /** Get data source relation for given table, keyspace, scanType, cluster and userSpecifiedSchema */
    def getDataSourceRelation(table: String,
                              keyspace: String,
                              scanType: ScanType = PrunedFilteredScanType,
                              cluster: Option[String] = None,
                              userSpecifiedSchema: Option[StructType] = None) : BaseRelationImpl = {
      scanType.makeRelation(table, keyspace, cluster, userSpecifiedSchema, sqlContext)

    }

    /** Get context Cassandra configuration settings. If it's not found, use default context configuration settings */
    private def getContextConfSettings(): CassandraConfCache =
      ConfManager.getContextConfSettings(sqlContext.getConf(SparkSQLContextNameProperty, DefaultSparkSQLContextName))

    /** remove context Cassandra configuration settings. */
    def removeContextConfSettings(): Unit =
      ConfManager.removeContextConfSettings(sqlContext.getConf(SparkSQLContextNameProperty))

    /** clear all context Cassandra configuration settings. */
    def clearContextConfSettings(): Unit =
      ConfManager.clearContextConfSettings()

    /** Add table level read configuration settings. Set cluster to None for a single cluster */
    def addTableReadConf(keyspace: String,
                         table: String,
                         conf: ReadConf,
                         cluster: Option[String]): Unit =
      getContextConfSettings().addTableReadConf(keyspace, table, conf, cluster)

    /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelReadConf(keyspace: String,
                                 conf: ReadConf,
                                 cluster: Option[String]): Unit =
      getContextConfSettings().addKeyspaceLevelReadConf(keyspace, conf, cluster)

    /** Add cluster level read configuration settings */
    def addClusterLevelReadConf(cluster: String, conf: ReadConf): Unit =
      getContextConfSettings().addClusterLevelReadConf(cluster, conf)

    /** Remove table level read configuration settings */
    def removeTableLevelReadConf(keyspace: String, table: String, cluster: Option[String]): Unit =
      getContextConfSettings().removeTableLevelReadConf(keyspace, table, cluster)

    /** Remove keyspace level read configuration settings */
    def removeKeyspaceLevelReadConf(keyspace: String, cluster: Option[String]): Unit =
      getContextConfSettings().removeKeyspaceLevelReadConf(keyspace, cluster)

    /** Remove cluster level read configuration settings */
    def removeClusterLevelReadConf(cluster: String): Unit =
      getContextConfSettings().removeClusterLevelReadConf(cluster)

    /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getReadConf(keyspace: String,
                    table: String,
                    cluster: Option[String]): ReadConf =
      getContextConfSettings().getReadConf(keyspace, table, cluster, ReadConf.fromSparkConf(sqlContext.sparkContext.getConf))

    /** Add table level write configuration settings. Set cluster to None for a single cluster */
    def addTableWriteConf(keyspace: String,
                          table: String,
                          conf: WriteConf,
                          cluster: Option[String]): Unit =
      getContextConfSettings().addTableWriteConf(keyspace, table, conf, cluster)

    /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelWriteConf(keyspace: String,
                                  writeConf: WriteConf,
                                  cluster: Option[String]) : Unit=
      getContextConfSettings().addKeyspaceLevelWriteConf(keyspace, writeConf, cluster)

    /** Add cluster level write configuration settings */
    def addClusterLevelWriteConf(cluster: String, conf: WriteConf) : Unit =
      getContextConfSettings().addClusterLevelWriteConf(cluster, conf)

    /** Remove table level write configuration settings */
    def removeTableLevelWriteConf(keyspace: String, table: String, cluster: Option[String]) : Unit =
      getContextConfSettings().removeTableLevelWriteConf(keyspace, table, cluster)

    /** Remove keyspace level write configuration settings */
    def removeKeyspaceLevelWriteConf(keyspace: String, cluster: Option[String]) : Unit =
      getContextConfSettings().removeKeyspaceLevelWriteConf(keyspace, cluster)

    /** Remove cluster level write configuration settings */
    def removeClusterLevelWriteConf(cluster: String) : Unit =
      getContextConfSettings().removeClusterLevelWriteConf(cluster)

    /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getWriteConf(keyspace: String,
                     table: String,
                     cluster: Option[String]): WriteConf =
      getContextConfSettings().getWriteConf(keyspace, table, cluster, WriteConf.fromSparkConf(sqlContext.sparkContext.getConf))

    /** Add cluster level write configuration settings */
    def addClusterLevelCassandraConnConf(cluster: String, conf: CassandraConnectorConf) : Unit =
      getContextConfSettings().addClusterLevelCassandraConnConf(cluster, conf)

    /** Remove cluster level write configuration settings */
    def removeClusterLevelCassandraConnConf(cluster: String) : Unit =
      getContextConfSettings().removeClusterLevelCassandraConnConf(cluster)

    /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
    def getCassandraConnConf(cluster: Option[String]) : CassandraConnectorConf =
      getContextConfSettings().getCassandraConnConf(cluster, CassandraConnectorConf(sqlContext.sparkContext.getConf))

    val schemas = CacheBuilder.newBuilder
      .maximumSize(100)
      .expireAfterWrite(sqlContext.sparkContext.getConf.getLong(CassandraSchemaExpireInMinutesProperty, DefaultCassandraSchemaExpireInMinutes), TimeUnit.MINUTES)
      .build(
        new CacheLoader[String, Schema] {
          def load(cluster: String) : Schema = {
            val clusterOpt = if("default".eq(cluster)) None else Option(cluster)
            Schema.fromCassandra(new CassandraConnector(sqlContext.getCassandraConnConf(clusterOpt)))
          }
        })
  }

  object CSQLContext {
    val CassandraSchemaExpireInMinutesProperty = "spark.cassandra.schema.expire.in.minutes"
    val DefaultCassandraSchemaExpireInMinutes = 10
    val SparkSQLContextNameProperty = "spark.sql.context.name"
    val DefaultSparkSQLContextName = "default"

    val Properties = Seq(
      CassandraSchemaExpireInMinutesProperty,
      SparkSQLContextNameProperty
    )
  }
}