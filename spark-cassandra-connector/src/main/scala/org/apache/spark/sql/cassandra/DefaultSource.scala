package org.apache.spark.sql.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{CreatableRelationProvider, SchemaRelationProvider, BaseRelation, RelationProvider}
import org.apache.spark.sql.types.StructType


class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  /** Creates a new relation for a cassandra table given table, keyspace, and cluster as a parameter.*/
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    sqlContext.getDataSourceRelation(getTable(parameters), getKeyspace(parameters),
      getScanType(parameters), getCluster(parameters), None)
  }

  /** Creates a new relation for a cassandra table given table, keyspace, cluster and schema as a parameter.*/
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    sqlContext.getDataSourceRelation(getTable(parameters), getKeyspace(parameters),
      getScanType(parameters), getCluster(parameters), Option(schema))
  }


  /** Creates a new relation for a cassandra table given table, keyspace, cluster, schema and data as a parameter.*/
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val table = sqlContext.getDataSourceRelation(getTable(parameters), getKeyspace(parameters),
      getScanType(parameters), getCluster(parameters), None)

    mode match {
      case Append => table.insert(data, false)
      case Overwrite => throw new UnsupportedOperationException("Overwriting a Cassandra Table is not allowed.")
      case ErrorIfExists =>
        if (!table.buildScan().isEmpty()) {
          throw new UnsupportedOperationException("Writing to a none-empty Cassandra Table is not allowed.")
        } else {
          table.insert(data, false)
        }
      case Ignore =>
    }

    sqlContext.getDataSourceRelation(getTable(parameters), getKeyspace(parameters),
      getScanType(parameters), getCluster(parameters), None)
  }


  import DefaultSource._

  private def getTable(parameters: Map[String, String]) =
    parameters.getOrElse(CassandraDataSourceTableNameProperty, missingProp(CassandraDataSourceTableNameProperty))

  private def getKeyspace(parameters: Map[String, String]) =
    parameters.getOrElse(CassandraDataSourceKeyspaceNameProperty, missingProp(CassandraDataSourceKeyspaceNameProperty))

  private def getCluster(parameters: Map[String, String]) = parameters.get(CassandraDataSourceClusterNameProperty)

  private def missingProp(prop: String) = throw new IllegalArgumentException(s"Missing $prop name")

  private def getScanType(parameters: Map[String, String]) = CassandraDataSourceScanTypeMap.get(
    parameters.getOrElse(CassandraDataSourceScanTypeNameProperty,
    CassandraDataSourcePrunedFilteredScanTypeName).toLowerCase).getOrElse(CatalystScanType)
}

object DefaultSource {
  val CassandraDataSourceTableNameProperty = "c_table"
  val CassandraDataSourceKeyspaceNameProperty = "keyspace"
  val CassandraDataSourceClusterNameProperty = "cluster"
  val CassandraDataSourceScanTypeNameProperty = "scan_type"

  val CassandraDataSourceBaseScanTypeName = "base"
  val CassandraDataSourcePrunedScanTypeName = "pruned"
  val CassandraDataSourcePrunedFilteredScanTypeName = "pruned_filtered"
  val CassandraDataSourceCatalystScanTypeName = "catalyst"
  val CassandraDataSourceScanTypeMap = Map (
    CassandraDataSourceBaseScanTypeName -> BaseScanType,
    CassandraDataSourcePrunedScanTypeName -> PrunedScanType,
    CassandraDataSourcePrunedFilteredScanTypeName -> PrunedFilteredScanType,
    CassandraDataSourceCatalystScanTypeName -> CatalystScanType
  )
}