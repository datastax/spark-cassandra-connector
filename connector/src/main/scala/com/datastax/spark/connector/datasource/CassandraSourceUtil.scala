package com.datastax.spark.connector.datasource

import java.util.Locale

import com.datastax.spark.connector.TableRef
import com.datastax.spark.connector.util.{ConfigParameter, DeprecatedConfigParameter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation

object CassandraSourceUtil {

  /**
    * Consolidate Cassandra conf settings in the order of
    * table level -> keyspace level -> cluster level ->
    * default. Use the first available setting. Default
    * settings are stored in SparkConf.
    */
  def consolidateConfs(
    sparkConf: SparkConf,
    sqlConf: Map[String, String],
    cluster: String = "default",
    keyspace: String = "",
    tableConf: Map[String, String] = Map.empty) : SparkConf = {

    //Default settings
    val conf = sparkConf.clone()
    val AllSCCConfNames = (ConfigParameter.names ++ DeprecatedConfigParameter.names)
    //Keyspace/Cluster level settings
    for (prop <- AllSCCConfNames) {
      val value = Seq(
        tableConf.get(prop.toLowerCase(Locale.ROOT)), //tableConf is actually a caseInsensitive map so lower case keys must be used
        sqlConf.get(s"$cluster:$keyspace/$prop"),
        sqlConf.get(s"$cluster/$prop"),
        sqlConf.get(s"default/$prop"),
        sqlConf.get(prop)).flatten.headOption
      value.foreach(conf.set(prop, _))
    }
    //Set all user properties
    conf.setAll(tableConf -- AllSCCConfNames)
    conf
  }
}
