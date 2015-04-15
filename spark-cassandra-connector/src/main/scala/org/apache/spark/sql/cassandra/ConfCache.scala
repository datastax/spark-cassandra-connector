package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.commons.lang.StringUtils

import scala.collection.mutable


object ConfCache {
  private val clusterReadConf = mutable.Map[String, ReadConf]()
  private val keyspaceReadConf = mutable.Map[Seq[String], ReadConf]()
  private val tableReadConf = mutable.Map[Seq[String], ReadConf]()

  /** Add table level read configuration settings. Set cluster to None for a single cluster */
  def addTableReadConf(keyspace: String,
                       table: String,
                       conf: ReadConf,
                       cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableReadConf += Seq(table, keyspace, c) -> conf
      case _       => tableReadConf += Seq(table, keyspace) -> conf
    }
  }

  /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelReadConf(keyspace: String,
                               conf: ReadConf,
                               cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceReadConf += Seq(keyspace, c) -> conf
      case _       => keyspaceReadConf += Seq(keyspace) -> conf
    }
  }

  /** Add cluster level read configuration settings */
  def addClusterLevelReadConf(cluster: String, conf: ReadConf) = {
    validateClusterName(cluster)
    clusterReadConf += cluster -> conf
  }

  /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getReadConf(keyspace: String,
                  table: String,
                  cluster: Option[String],
                  defaultConf: ReadConf): ReadConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableReadConf.get(Seq(table, keyspace, c)).getOrElse(
          keyspaceReadConf.get(Seq(keyspace, c)).getOrElse(
            clusterReadConf.get(c).getOrElse(defaultConf)))
      case _       => tableReadConf.get(Seq(table, keyspace)).getOrElse(
        keyspaceReadConf.get(Seq(keyspace)).getOrElse(defaultConf))
    }
  }

  private val clusterWriteConf = mutable.Map[String, WriteConf]()
  private val keyspaceWriteConf = mutable.Map[Seq[String], WriteConf]()
  private val tableWriteConf = mutable.Map[Seq[String], WriteConf]()

  /** Add table level write configuration settings. Set cluster to None for a single cluster */
  def addTableWriteConf(keyspace: String,
                        table: String,
                        conf: WriteConf,
                        cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableWriteConf += Seq(table, keyspace, c) -> conf
      case _       => tableWriteConf += Seq(table, keyspace) -> conf
    }
  }

  /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelWriteConf(keyspace: String,
                                writeConf: WriteConf,
                                cluster: Option[String]) = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceWriteConf += Seq(keyspace, c) -> writeConf
      case _       => keyspaceWriteConf += Seq(keyspace) -> writeConf
    }
  }

  /** Add cluster level write configuration settings */
  def addClusterLevelWriteConf(cluster: String, conf: WriteConf) = {
    validateClusterName(cluster)
    clusterWriteConf += cluster -> conf
  }

  /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getWriteConf(keyspace: String,
                   table: String,
                   cluster: Option[String],
                   defaultConf: WriteConf): WriteConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableWriteConf.get(Seq(table, keyspace, c)).getOrElse(
          keyspaceWriteConf.get(Seq(keyspace, c)).getOrElse(
            clusterWriteConf.get(c).getOrElse(defaultConf)))
      case _       => tableWriteConf.get(Seq(table, keyspace)).getOrElse(
        keyspaceWriteConf.get(Seq(keyspace)).getOrElse(defaultConf))
    }
  }

  private val clusterCassandraConnConf = mutable.Map[String, CassandraConnectorConf]()

  /** Add cluster level write configuration settings */
  def addClusterLevelCassandraConnConf(cluster: String, conf: CassandraConnectorConf) = {
    validateClusterName(cluster)
    clusterCassandraConnConf += cluster -> conf
  }

  /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
  def getCassandraConnConf(cluster: Option[String],
                            defaultConf: CassandraConnectorConf): CassandraConnectorConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
        clusterCassandraConnConf.get(c).getOrElse(
          throw new RuntimeException(s"Missing cluster $c Cassandra connection conf"))
      case _       => defaultConf
    }
  }

  private def validateClusterName(cluster: String) = {
    if (StringUtils.isEmpty(cluster)) {
      throw new IllegalArgumentException("cluster name can't be null or empty")
    }
  }

}
