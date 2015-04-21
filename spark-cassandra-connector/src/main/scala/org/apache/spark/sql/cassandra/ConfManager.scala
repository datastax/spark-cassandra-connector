package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.commons.lang.StringUtils

object ConfManager {
  import CSQLContext._

  /** A map stores Cassandra [[CassandraConfCache]] per context */
  private val confSettings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, CassandraConfCache](){
      put(DefaultSparkSQLContextName, new CassandraConfCache())
    })

  /** Add a Cassandra [[CassandraConfCache]] for a context to the cache */
  def addContextConfSettings(contextName: String): Unit = {
    confSettings.put(contextName, new CassandraConfCache())
  }

  /** Remove the Cassandra configuration settings for all contexts */
  def clearContextConfSettings() : Unit = {
    confSettings.clear()
  }

  /** Remove Cassandra configuration settings for the context */
  def removeContextConfSettings(contextName: String) : Unit = {
    confSettings.remove(contextName)
  }

  /** Get Cassandra configuration settings for the context, if it's not found, use default context settings */
  def getContextConfSettings(contextName: String): CassandraConfCache = {
    if (confSettings.get(contextName) == null) {
      copyFromDefaultContext(contextName)
    }
    confSettings.get(contextName)
  }

  /** Copy the default Cassandra context settings to the context */
  def copyFromDefaultContext(contextName: String): Unit = {
    val defaultSettings = confSettings.get(DefaultSparkSQLContextName)
    if (defaultSettings == null) {
      val cache = new CassandraConfCache()
      confSettings.put(DefaultSparkSQLContextName, cache)
      confSettings.put(contextName, cache)
    } else {
      confSettings.put(contextName, defaultSettings)
    }
  }
}

class CassandraConfCache {

  private val clusterReadConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ReadConf]())
  private val keyspaceReadConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Seq[String], ReadConf]())
  private val tableReadConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Seq[String], ReadConf]())

  /** Add table level read configuration settings. Set cluster to None for a single cluster */
  def addTableReadConf(keyspace: String,
                       table: String,
                       conf: ReadConf,
                       cluster: Option[String]): Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableReadConf.put(Seq(table, keyspace, c), conf)
      case _       => tableReadConf.put(Seq(table, keyspace), conf)
    }
  }

  /** Add keyspace level read configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelReadConf(keyspace: String,
                               conf: ReadConf,
                               cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceReadConf.put(Seq(keyspace, c), conf)
      case _       => keyspaceReadConf.put(Seq(keyspace), conf)
    }
  }

  /** Add cluster level read configuration settings */
  def addClusterLevelReadConf(cluster: String, conf: ReadConf) : Unit = {
    validateClusterName(cluster)
    clusterReadConf.put(cluster, conf)
  }

  /** Remove table level read configuration settings */
  def removeTableLevelReadConf(keyspace: String, table: String, cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableReadConf.remove(Seq(table, keyspace, c))
      case _       => tableReadConf.remove(Seq(table, keyspace))
    }
  }

  /** Remove keyspace level read configuration settings */
  def removeKeyspaceLevelReadConf(keyspace: String, cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceReadConf.remove(Seq(keyspace, c))
      case _       => keyspaceReadConf.remove(Seq(keyspace))
    }
  }

  /** Remove cluster level read configuration settings */
  def removeClusterLevelReadConf(cluster: String) : Unit = {
    clusterReadConf.remove(cluster)
  }

  /** Get read configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getReadConf(keyspace: String,
                  table: String,
                  cluster: Option[String],
                  defaultConf: ReadConf): ReadConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
        Option(tableReadConf.get(Seq(table, keyspace, c))).getOrElse(
          Option(keyspaceReadConf.get(Seq(keyspace, c))).getOrElse(
            Option(clusterReadConf.get(c)).getOrElse(defaultConf)))
      case _       => Option(tableReadConf.get(Seq(table, keyspace))).getOrElse(
        Option(keyspaceReadConf.get(Seq(keyspace))).getOrElse(defaultConf))
    }
  }

  private val clusterWriteConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, WriteConf]())
  private val keyspaceWriteConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Seq[String], WriteConf]())
  private val tableWriteConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Seq[String], WriteConf]())

  /** Add table level write configuration settings. Set cluster to None for a single cluster */
  def addTableWriteConf(keyspace: String,
                        table: String,
                        conf: WriteConf,
                        cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableWriteConf.put(Seq(table, keyspace, c), conf)
      case _       => tableWriteConf.put(Seq(table, keyspace), conf)
    }
  }

  /** Add keyspace level write configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelWriteConf(keyspace: String,
                                writeConf: WriteConf,
                                cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceWriteConf.put(Seq(keyspace, c),  writeConf)
      case _       => keyspaceWriteConf.put(Seq(keyspace), writeConf)
    }
  }

  /** Add cluster level write configuration settings */
  def addClusterLevelWriteConf(cluster: String, conf: WriteConf) : Unit = {
    validateClusterName(cluster)
    clusterWriteConf.put(cluster, conf)
  }

  /** Remove table level write configuration settings */
  def removeTableLevelWriteConf(keyspace: String, table: String, cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        tableWriteConf.remove(Seq(table, keyspace, c))
      case _       => tableWriteConf.remove(Seq(table, keyspace))
    }
  }

  /** Remove cluster level write configuration settings */
  def removeKeyspaceLevelWriteConf(keyspace: String, cluster: Option[String]) : Unit = {
    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceWriteConf.remove(Seq(keyspace, c))
      case _       => keyspaceWriteConf.remove(Seq(keyspace))
    }
  }

  /** Remove cluster level write configuration settings */
  def removeClusterLevelWriteConf(cluster: String) : Unit = {
    clusterWriteConf.remove(cluster)
  }

  /** Get write configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getWriteConf(keyspace: String,
                   table: String,
                   cluster: Option[String],
                   defaultConf: WriteConf): WriteConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
        Option(tableWriteConf.get(Seq(table, keyspace, c))).getOrElse(
          Option(keyspaceWriteConf.get(Seq(keyspace, c))).getOrElse(
            Option(clusterWriteConf.get(c)).getOrElse(defaultConf)))
      case _       => Option(tableWriteConf.get(Seq(table, keyspace))).getOrElse(
        Option(keyspaceWriteConf.get(Seq(keyspace))).getOrElse(defaultConf))
    }
  }

  private val clusterCassandraConnConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, CassandraConnectorConf]())

  /** Add cluster level write configuration settings */
  def addClusterLevelCassandraConnConf(cluster: String, conf: CassandraConnectorConf) : Unit = {
    validateClusterName(cluster)
    clusterCassandraConnConf.put(cluster, conf)
  }

  /** Remove cluster level write configuration settings */
  def removeClusterLevelCassandraConnConf(cluster: String) : Unit = {
    validateClusterName(cluster)
    clusterCassandraConnConf.remove(cluster)
  }

  /** Get Cassandra connection configuration settings by the order of cluster level, default settings */
  def getCassandraConnConf(cluster: Option[String],
                           defaultConf: CassandraConnectorConf): CassandraConnectorConf = {
    cluster match {
      case Some(c) => validateClusterName(c)
        Option(clusterCassandraConnConf.get(c)).getOrElse(
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
