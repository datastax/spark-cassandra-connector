package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import com.datastax.spark.connector.cql.{CassandraConnector, Schema, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.apache.spark.sql.types.StructType

package object cassandra {

  /**
   * Adds a method, `cassandraTable`, to SQLContext that allows reading data stored in Cassandra.
   */
  implicit class CSQLContext(sqlContext: SQLContext) {

    /**
     * Create a DataFrame for a given Cassandra table of a keyspace and optional cluster, user defined schema
     * and scan type including [[PrunedFilteredScanType]], [[BaseScanType]], [[PrunedScanType]]
     * and [[CatalystScanType]]
     * */
    def cassandraTable(table: String,
                       keyspace: String,
                       scanType: ScanType = PrunedFilteredScanType,
                       cluster: Option[String] = None,
                       userSpecifiedSchema: Option[StructType] = None) =
      sqlContext.baseRelationToDataFrame(getDataSourceRelation(table, keyspace, scanType, cluster, userSpecifiedSchema))

    def getDataSourceRelation(table: String,
                              keyspace: String,
                              scanType: ScanType = PrunedFilteredScanType,
                              cluster: Option[String] = None,
                              userSpecifiedSchema: Option[StructType] = None) = {
      scanType match {
        case BaseScanType => CSBaseScanRelation(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
        case PrunedScanType => CSPrunedScanRelation(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
        case PrunedFilteredScanType => CSPrunedFilteredScanRelation(table,
          keyspace, cluster, userSpecifiedSchema, sqlContext)
        case CatalystScanType => CSCatalystScanRelation(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
      }
    }

    /** Add table level read configuration settings. Set cluster to None for a single cluster */
    def addTableReadConf(keyspace: String,
                         table: String,
                         conf: ReadConf,
                         cluster: Option[String]) = ConfCache.addTableReadConf(keyspace, table, conf, cluster)

    /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelReadConf(keyspace: String,
                                 conf: ReadConf,
                                 cluster: Option[String]) =
      ConfCache.addKeyspaceLevelReadConf(keyspace, conf, cluster)

    /** Add cluster level read configuration settings */
    def addClusterLevelReadConf(cluster: String, conf: ReadConf) =
      ConfCache.addClusterLevelReadConf(cluster, conf)

    /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getReadConf(keyspace: String,
                    table: String,
                    cluster: Option[String]): ReadConf =
      ConfCache.getReadConf(keyspace, table, cluster, ReadConf.fromSparkConf(sqlContext.sparkContext.getConf))

    /** Add table level write configuration settings. Set cluster to None for a single cluster */
    def addTableWriteConf(keyspace: String,
                          table: String,
                          conf: WriteConf,
                          cluster: Option[String]) =
      ConfCache.addTableWriteConf(keyspace, table, conf, cluster)

    /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
    def addKeyspaceLevelWriteConf(keyspace: String,
                                  writeConf: WriteConf,
                                  cluster: Option[String]) =
      ConfCache.addKeyspaceLevelWriteConf(keyspace, writeConf, cluster)

    /** Add cluster level write configuration settings */
    def addClusterLevelWriteConf(cluster: String, conf: WriteConf) =
      ConfCache.addClusterLevelWriteConf(cluster, conf)

    /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
    def getWriteConf(keyspace: String,
                     table: String,
                     cluster: Option[String]): WriteConf =
      ConfCache.getWriteConf(keyspace, table, cluster, WriteConf.fromSparkConf(sqlContext.sparkContext.getConf))

    /** Add cluster level write configuration settings */
    def addClusterLevelCassandraConnConf(cluster: String, conf: CassandraConnectorConf) =
      ConfCache.addClusterLevelCassandraConnConf(cluster, conf)

    /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
    def getCassandraConnConf(cluster: Option[String]) =
      ConfCache.getCassandraConnConf(cluster, CassandraConnectorConf(sqlContext.sparkContext.getConf))

    val schemas = CacheBuilder.newBuilder
      .maximumSize(100)
      .expireAfterWrite(sqlContext.sparkContext.getConf.getLong("schema.expire.in.minutes", 10), TimeUnit.MINUTES)
      .build(
        new CacheLoader[String, Schema] {
          def load(cluster: String) : Schema = {
            val clusterOpt = toOption(cluster)
            Schema.fromCassandra(new CassandraConnector(sqlContext.getCassandraConnConf(clusterOpt)))
          }
        })

    private def toOption(cluster: String): Option[String] = {
      cluster match {
        case "default" => None
        case _         => Option(cluster)
      }
    }
  }
}