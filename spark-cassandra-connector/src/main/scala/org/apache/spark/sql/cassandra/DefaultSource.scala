package org.apache.spark.sql.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{CreatableRelationProvider, SchemaRelationProvider, BaseRelation, RelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Cassandra data source extends [[RelationProvider]], [[SchemaRelationProvider]] and [[CreatableRelationProvider]].
 * It's used internally by Spark SQL to create Relation when create a table which specify the Cassandra data source
 * e.g.
 *
 *      CREATE TEMPORARY TABLE tmpTable
 *      USING org.apache.spark.sql.cassandra
 *      OPTIONS (
 *       c_table "table",
 *       keyspace "keyspace",
 *       scan_type "catalyst",
 *       schema '{"type":"struct","fields":
 *        [{"name":"a","type":"integer","nullable":true,"metadata":{}},
 *         {"name":"b","type":"integer","nullable":true,"metadata":{}},
 *         {"name":"c","type":"integer","nullable":true,"metadata":{}}]}'
 *      )
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  /** Creates a new relation for a cassandra table given table, keyspace, and cluster as a parameter.*/
  override def createRelation(
               sqlContext: SQLContext,
               parameters: Map[String, String]): BaseRelation = {

    val option = DataSourceOption(parameters)
    val schema = option.schemaJsonString.map(DataType.fromJson).map(_.asInstanceOf[StructType])
    sqlContext.getDataSourceRelation(
      option.tableName,
      option.keyspaceName,
      option.scanType,
      schema,
      option.clusterName)
  }

  /** Creates a new relation for a cassandra table given table, keyspace, cluster and schema as a parameter.*/
  override def createRelation(
               sqlContext: SQLContext,
               parameters: Map[String, String],
               schema: StructType): BaseRelation = {

    val option = DataSourceOption(parameters)
    sqlContext.getDataSourceRelation(
      option.tableName,
      option.keyspaceName,
      option.scanType,
      Option(schema),
      option.clusterName)
  }


  /** Creates a new relation for a cassandra table given table, keyspace, cluster, schema and data as a parameter.*/
  override def createRelation(
               sqlContext: SQLContext,
               mode: SaveMode,
               parameters: Map[String, String],
               data: DataFrame): BaseRelation = {

    val option = DataSourceOption(parameters)
    val table = sqlContext.getDataSourceRelation(
      option.tableName,
      option.keyspaceName,
      option.scanType,
      None,
      option.clusterName)

    val notOverwrite = false
    mode match {
      case Append => table.insert(data, notOverwrite)
      case Overwrite => throw new UnsupportedOperationException("Overwriting a Cassandra Table is not allowed.")
      case ErrorIfExists =>
        if (!table.buildScan().isEmpty()) {
          throw new UnsupportedOperationException("Writing to a none-empty Cassandra Table is not allowed.")
        } else {
          table.insert(data, notOverwrite)
        }
      case Ignore =>
    }

    sqlContext.getDataSourceRelation(
      option.tableName,
      option.keyspaceName,
      option.scanType,
      None,
      option.clusterName)
  }

  private case class DataSourceOption(
                     parameters: Map[String, String]) {

    import DefaultSource._

    val scanType : ScanType = {
      val stn = getScanTypeName(parameters).toLowerCase
      CassandraDataSourceScanTypeMap.get(stn).get
    }

    val tableName =
      parameters.getOrElse(CassandraDataSourceTableNameProperty, missingProp(CassandraDataSourceTableNameProperty))

    val keyspaceName =
      parameters.getOrElse(CassandraDataSourceKeyspaceNameProperty, missingProp(CassandraDataSourceKeyspaceNameProperty))

    val clusterName = parameters.get(CassandraDataSourceClusterNameProperty)

    val schemaJsonString = parameters.get(CassandraDataSourUserDefinedSchemaNameProperty)

    private def missingProp(
                prop: String) = {

      throw new IllegalArgumentException(s"Missing $prop name")
    }

    private def getScanTypeName(
                parameters: Map[String, String]): String = {

      parameters.get(CassandraDataSourceScanTypeNameProperty)
        .getOrElse(CassandraDataSourcePrunedFilteredScanTypeName)
    }
  }
}


object DefaultSource {
  val CassandraDataSourceTableNameProperty = "c_table"
  val CassandraDataSourceKeyspaceNameProperty = "keyspace"
  val CassandraDataSourceClusterNameProperty = "cluster"
  val CassandraDataSourceScanTypeNameProperty = "scan_type"
  val CassandraDataSourUserDefinedSchemaNameProperty = "schema"

  val CassandraDataSourceBaseScanTypeName = "base"
  val CassandraDataSourcePrunedScanTypeName = "pruned"
  val CassandraDataSourcePrunedFilteredScanTypeName = "pruned_filtered"
  val CassandraDataSourceCatalystScanTypeName = "catalyst"
  val CassandraDataSourceScanTypeMap : Map[String, ScanType] = Map (
    CassandraDataSourceBaseScanTypeName -> BaseScanType,
    CassandraDataSourcePrunedScanTypeName -> PrunedScanType,
    CassandraDataSourcePrunedFilteredScanTypeName -> PrunedFilteredScanType,
    CassandraDataSourceCatalystScanTypeName -> CatalystScanType
  ).withDefaultValue(CatalystScanType)
}