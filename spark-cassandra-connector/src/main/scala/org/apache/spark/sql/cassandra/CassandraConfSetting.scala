package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.commons.lang.StringUtils

/**
 * A local cache to store user defined Cassandra connection configuration settings. It includes
 * per cluster level, per keyspace level and per table level settings. It also allows to
 * update the settings.
 */
class CassandraConnConfSetting extends CassandraConfSetting[CassandraConnectorConf] {

  import CassandraConfSetting._

  /** Get Cassandra connection configuration settings for a cluster */
  def getConf(
      cluster: Option[String],
      defaultConf: CassandraConnectorConf): CassandraConnectorConf = {

    cluster match {
      case Some(c) => validateClusterName(c)
        Option(clusterConf.get(c)).getOrElse(
          throw new RuntimeException(s"Missing cluster $c Cassandra connection conf"))
      case _ => defaultConf
    }
  }

  override def addTableConf(
               table: String,
               keyspace: String,
               cluster: Option[String],
               conf: CassandraConnectorConf): Unit = {

    throw new UnsupportedOperationException("Add table level CassandraConnectorConf is not supported")
  }

  override def addKeyspaceLevelConf(
               keyspace: String,
               cluster: Option[String],
               conf: CassandraConnectorConf): Unit = {

    throw new UnsupportedOperationException("Add keyspace level CassandraConnectorConf is not supported")
  }

  override def removeTableLevelConf(
               table: String,
               keyspace: String,
               cluster: Option[String]): Unit = {

    throw new UnsupportedOperationException("remove table level CassandraConnectorConf is not supported")
  }

  override def removeKeyspaceLevelConf(
               keyspace: String,
               cluster: Option[String]) : Unit = {

    throw new UnsupportedOperationException("remove keyspace level CassandraConnectorConf is not supported")
  }

  override def getConf(
               table: String,
               keyspace: String,
               cluster: Option[String],
               defaultConf: CassandraConnectorConf): CassandraConnectorConf = {

    throw new UnsupportedOperationException("get table/keyspace level CassandraConnectorConf is not supported")

  }
}

/**
 * A local cache to store user defined Cassandra configuration settings. It includes
 * per cluster level, per keyspace level and per table level settings. It also allows to
 * update the settings. It's mainly used by Spark SQL, so user can define all kind of
 * Cassandra configuration settings, and SQL query automatically get the settings to
 * connect to, read from and write to Cassandra.
 *
 * Potentially it could be used by other components as well.
 *
 * If it's difficult to isolate the settings for your application, you still could pass
 * explicitly those settings to the methods.
 */
class CassandraConfSetting[T] {

  import CassandraConfSetting._

  /** Low lock contention so use [[java.util.Collections.synchronizedMap]] */
  protected val clusterConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, T]())
  private val keyspaceConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Seq[String], T]())
  private val tableConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[Seq[String], T]())

  /** Add table level configuration settings. Set cluster to None for a single cluster */
  def addTableConf(
      table: String,
      keyspace: String,
      cluster: Option[String],
      conf: T): Unit = {

    cluster match {
      case Some(c) => validateClusterName(c)
        tableConf.put(Seq(table, keyspace, c), conf)
      case _ => tableConf.put(Seq(table, keyspace), conf)
    }
  }

  /** Add keyspace level configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelConf(
      keyspace: String,
      cluster: Option[String],
      conf: T) : Unit = {

    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceConf.put(Seq(keyspace, c), conf)
      case _ => keyspaceConf.put(Seq(keyspace), conf)
    }
  }

  /** Add cluster level configuration settings */
  def addClusterLevelConf(
      cluster: String,
      conf: T) : Unit = {

    validateClusterName(cluster)
    clusterConf.put(cluster, conf)
  }

  /** Remove table level configuration settings */
  def removeTableLevelConf(
      table: String,
      keyspace: String,
      cluster: Option[String]) : Unit = {

    cluster match {
      case Some(c) => validateClusterName(c)
        tableConf.remove(Seq(table, keyspace, c))
      case _  => tableConf.remove(Seq(table, keyspace))
    }
  }

  /** Remove keyspace level configuration settings */
  def removeKeyspaceLevelConf(
      keyspace: String,
      cluster: Option[String]) : Unit = {

    cluster match {
      case Some(c) => validateClusterName(c)
        keyspaceConf.remove(Seq(keyspace, c))
      case _ => keyspaceConf.remove(Seq(keyspace))
    }
  }

  /** Remove cluster level configuration settings */
  def removeClusterLevelConf(
      cluster: String) : Unit = {

    clusterConf.remove(cluster)
  }

  /** Get configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getConf(
      table: String,
      keyspace: String,
      cluster: Option[String],
      defaultConf: T): T = {

    cluster match {
      case Some(c) => validateClusterName(c)
        Option(tableConf.get(Seq(table, keyspace, c))).getOrElse(
          Option(keyspaceConf.get(Seq(keyspace, c))).getOrElse(
            Option(clusterConf.get(c)).getOrElse(defaultConf)))
      case _       => Option(tableConf.get(Seq(table, keyspace))).getOrElse(
        Option(keyspaceConf.get(Seq(keyspace))).getOrElse(defaultConf))
    }
  }
}

object CassandraConfSetting {

  def validateClusterName(
      cluster: String) = {

    if (StringUtils.isEmpty(cluster)) {
      throw new IllegalArgumentException("cluster name can't be null or empty")
    }
  }
}
