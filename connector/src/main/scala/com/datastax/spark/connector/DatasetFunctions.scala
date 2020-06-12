package com.datastax.spark.connector

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.mapper.DataFrameColumnMapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.cassandra.{AlwaysOn, CassandraSourceRelation, DirectJoinSetting}

/** Provides Cassandra-specific methods on [[org.apache.spark.sql.DataFrame]] */
class DatasetFunctions[K: Encoder](dataset: Dataset[K]) extends Serializable {

  val sparkContext: SparkContext = (dataset.sqlContext.sparkContext)

  def directJoin(directJoinSetting: DirectJoinSetting= AlwaysOn): Dataset[K] = {
    CassandraSourceRelation.setDirectJoin(dataset, directJoinSetting)
  }

  /**
   *  Creates a C* table based on the Dataset Struct provided. Optionally
   *  takes in a list of partition columns or clustering columns names. When absent
   *  the first column will be used as the partition key and there will be no clustering
   *  keys.
   */
  def createCassandraTable(keyspaceName: String,
                           tableName: String,
                           partitionKeyColumns: Option[Seq[String]] = None,
                           clusteringKeyColumns: Option[Seq[(String, ClusteringOrder)]] = None,
                           tableOptions: Map[String, String] = Map())
                          (implicit connector: CassandraConnector = CassandraConnector(sparkContext)): Unit = {

    val protocolVersion = connector.withSessionDo(_.getContext.getProtocolVersion)
    val partitionKeys = partitionKeyColumns.map(_.toSet).getOrElse(Set[String]())
    val clusteringKeys = clusteringKeyColumns.map(_.toSet).getOrElse(Set[(String, ClusteringOrder)]())

    val originalTable = new DataFrameColumnMapper(dataset.schema).newTable(keyspaceName, tableName, protocolVersion)
    val newColumns = originalTable.cols.map(col => {
      col.copy(
        isPartitionKey = partitionKeys(col.name),
        clusteringKey = clusteringKeys.filter(_._1 == col.name).headOption.map(_._2)
      )
    })
    val newPartitionKeys = newColumns.filter(_.isPartitionKey).map(_.name).toSet
    val newClusteringKeys = newColumns.filter(_.clusteringKey.isDefined).map(_.name).toSet

    val missedPartitionKeys = partitionKeys -- newPartitionKeys
    if (!missedPartitionKeys.isEmpty) {
      throw new IllegalArgumentException(
        s""""Columns $missedPartitionKeys" not Found. Unable to make specified column(s) partition key(s).
           |Available Columns: ${originalTable.cols.map(_.name)}""".stripMargin)
    }
    val missedClusteringKeys = clusteringKeys.map(_._1) -- newClusteringKeys
    if (!missedClusteringKeys.isEmpty) {
      throw new IllegalArgumentException(
        s""""Columns $missedClusteringKeys" not Found. Unable to make specified column(s) clustering key(s).
           |Available Columns: ${originalTable.cols.map(_.name)}""".stripMargin)
    }

    val newTable = originalTable.copy(
      cols = newColumns,
      ifNotExists = false,
      options = tableOptions
    )

    connector.withSessionDo(session => session.execute(newTable.cql))
  }
}
