package com.datastax.spark.connector

import com.datastax.spark.connector.cql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/** Provides Cassandra-specific methods on [[org.apache.spark.sql.DataFrame]] */
class DataFrameFunctions(dataFrame: DataFrame) extends Serializable {

  val sparkContext: SparkContext = dataFrame.sqlContext.sparkContext

  /**
    *  Creates a C* table based on the DataFrame Struct provided. Optionally
    *  takes in a list of partition columns or clustering columns names. When absent
    *  the first column will be used as the partition key and there will be no clustering
    *  keys.
    */
  def createCassandraTable(
    keyspaceName: String,
    tableName: String,
    partitionKeyColumns: Option[Seq[String]] = None,
    clusteringKeyColumns: Option[Seq[String]] = None)(
  implicit
    connector: CassandraConnector = CassandraConnector(sparkContext.getConf)): Unit = {

    val protocolVersion = connector.
      withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersion)

    val rawTable = TableDef.fromDataFrame(dataFrame, keyspaceName, tableName, protocolVersion)
    val columnMapping = rawTable.columnByName

    val columnNames = columnMapping.keys.toSet
    val partitionKeyNames = partitionKeyColumns.getOrElse(rawTable.partitionKey.map(_.columnName))
    val clusteringKeyNames = clusteringKeyColumns.getOrElse(Seq.empty)
    val regularColumnNames = (columnNames -- (partitionKeyNames ++ clusteringKeyNames)).toSeq

    def missingColumnException(columnName: String, columnType: String) = {
      new IllegalArgumentException(
        s""""$columnName" not Found. Unable to make specified column $columnName a $columnType.
          |Available Columns: $columnNames""".stripMargin)

    }

    val table = rawTable.copy (
      partitionKey = partitionKeyNames
          .map(partitionKeyName =>
            columnMapping.getOrElse(partitionKeyName,
              throw missingColumnException(partitionKeyName, "Partition Key Column")))
          .map(_.copy(columnRole = PartitionKeyColumn))
      ,
      clusteringColumns = clusteringKeyNames
          .map(clusteringKeyName =>
            columnMapping.getOrElse(clusteringKeyName,
              throw missingColumnException(clusteringKeyName, "Clustering Column")))
          .zipWithIndex
          .map { case (col, index) => col.copy(columnRole = ClusteringColumn(index))}
      ,
      regularColumns = regularColumnNames
          .map(regularColumnName =>
             columnMapping.getOrElse(regularColumnName,
                  throw missingColumnException(regularColumnName, "Regular Column")))
          .map(_.copy(columnRole = RegularColumn))
    )

    connector.withSessionDo(session => session.execute(table.cql))
  }

}
