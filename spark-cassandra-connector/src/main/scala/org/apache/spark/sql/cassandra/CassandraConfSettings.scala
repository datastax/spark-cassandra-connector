package org.apache.spark.sql.cassandra

import scala.collection.concurrent.TrieMap


/**
 * Store user defined cluster level configuration settings.
 * It also updates the settings.
 */
class CassandraClusterLevelConfSettings[T] {

  protected val clusterConf = TrieMap[String, T]()

  /** Add cluster level configuration settings */
  def addClusterLevelConf(cluster: String, conf: T) : Unit = {
    validateName(cluster)
    clusterConf.put(cluster, conf)
  }

  /** Remove cluster level configuration settings */
  def removeClusterLevelConf(cluster: String) : Unit = {
    validateName(cluster)
    clusterConf.remove(cluster)
  }

  /** Get cluster level configuration settings*/
  def getClusterLevelConf(cluster: Option[String], defaultConf: T): T = {
    if (cluster == None) {
      defaultConf
    } else {
      validateName(cluster.get)
      clusterConf.getOrElse(cluster.get, defaultConf)
    }
  }

  protected def validateName(cluster: String) : Unit = {
    require(!empty(Option(cluster)), "Name can't be empty String or null")
  }

  protected def empty(cluster: Option[String]) : Boolean = {
    cluster == None || !cluster.exists(_.trim.nonEmpty)
  }
}


/**
 * Store user defined Cassandra configuration settings. It includes
 * per cluster level, per keyspace level and per table level settings. It also
 * updates the settings. It's mainly used by Spark SQL, so user can define all kind of
 * Cassandra configuration settings, and SQL query automatically get the settings to
 * connect to, read from and write to Cassandra.
 *
 * Potentially it could be used by other components as well.
 *
 * If it's difficult to isolate the settings for your application, pass
 * explicitly those settings to the methods.
 */
class CassandraConfSettings[T] extends CassandraClusterLevelConfSettings[T] {

  private val keyspaceConf = TrieMap[Seq[String], T]()
  private val tableConf = TrieMap[Seq[String], T]()

  /** Add table level configuration settings. Set cluster to None for a single cluster */
  def addTableConf(tableIdent: TableIdent, conf: T): Unit = {
    val table = tableIdent.table
    val keyspace = tableIdent.keyspace
    val cluster = tableIdent.cluster
    validateName(table, keyspace)
    if (empty(cluster)) {
      tableConf.put(Seq(table, keyspace), conf)
    } else {
      tableConf.put(Seq(table, keyspace, cluster.get), conf)
    }
  }

  /** Add keyspace level configuration settings. Set cluster to None for a single cluster */
  def addKeyspaceLevelConf(keyspace: String, cluster: Option[String], conf: T) : Unit = {
    validateName(keyspace)
    if (empty(cluster)) {
      keyspaceConf.put(Seq(keyspace), conf)
    } else {
      keyspaceConf.put(Seq(keyspace, cluster.get), conf)
    }
  }

  /** Remove table level configuration settings */
  def removeTableLevelConf(tableIdent: TableIdent): Unit = {
    val table = tableIdent.table
    val keyspace = tableIdent.keyspace
    val cluster = tableIdent.cluster
    validateName(table, keyspace)
    if (empty(cluster)) {
      tableConf.remove(Seq(table, keyspace))
    } else {
      tableConf.remove(Seq(table, keyspace, cluster.get))
    }
  }

  /** Remove keyspace level configuration settings */
  def removeKeyspaceLevelConf(keyspace: String, cluster: Option[String]) : Unit = {
    validateName(keyspace)
    if (empty(cluster)) {
      keyspaceConf.remove(Seq(keyspace))
    } else {
      keyspaceConf.remove(Seq(keyspace, cluster.get))
    }
  }

  /** Get configuration settings by the order of table level, keyspace level, cluster level, default settings */
  def getConf(tableIdent: TableIdent, defaultConf: T): T = {
    val table = tableIdent.table
    val keyspace = tableIdent.keyspace
    val cluster = tableIdent.cluster
    validateName(table, keyspace)
    if (empty(cluster)) {
      tableConf.getOrElse(Seq(table, keyspace),
        keyspaceConf.getOrElse(Seq(keyspace), defaultConf))
    } else {
      val clusterName = cluster.get
      tableConf.getOrElse(Seq(table, keyspace, clusterName),
        keyspaceConf.getOrElse(Seq(keyspace, clusterName),
          clusterConf.getOrElse(clusterName, defaultConf)))
    }
  }

  private def validateName(table: String, keyspace: String) : Unit = {
    validateName(table)
    validateName(keyspace)
  }

}

