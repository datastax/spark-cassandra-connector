package org.apache.spark.sql.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{CreatableRelationProvider, SchemaRelationProvider, BaseRelation, RelationProvider}
import org.apache.spark.sql.types.{DataType, StructType}
import CassandraDefaultSource._

/**
 * Cassandra data source extends [[RelationProvider]], [[SchemaRelationProvider]] and [[CreatableRelationProvider]].
 * It's used internally by Spark SQL to create Relation for a table which specifies the Cassandra data source
 * e.g.
 *
 *      CREATE TEMPORARY TABLE tmpTable
 *      USING org.apache.spark.sql.cassandra
 *      OPTIONS (
 *       c_table "table",
 *       keyspace "keyspace",
 *       cluster "test_cluster",
 *       push_down "true",
 *       schema '{"type":"struct","fields":
 *        [{"name":"a","type":"integer","nullable":true,"metadata":{}},
 *         {"name":"b","type":"integer","nullable":true,"metadata":{}},
 *         {"name":"c","type":"integer","nullable":true,"metadata":{}}]}'
 *      )
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  /**
   * Creates a new relation for a cassandra table.
   *
   * c_table, keyspace, cluster, push_down and schema can be specified in the parameters map.
   *
   * push_down must be true/false. When it's enable, some filters are pushed down to data source connector.
   * Other filters are applied to the data returned from data source connector
   *
   * schema must be in JSON format of [[StructType]].
   *
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    val (tableIdent, options) = parseParameters(parameters)
    sqlContext.createCassandraSourceRelation(tableIdent, options)
  }

  /**
   * Creates a new relation for a cassandra table given table, keyspace, cluster and push_down
   * as parameters and explicitly pass schema [[StructType]] as a parameter
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = {

    val (tableIdent, options) = parseParameters(parameters)
    val optionsWithNewSchema = CassandraDataSourceOptions(Option(schema), options.pushdown)
    sqlContext.createCassandraSourceRelation(tableIdent, optionsWithNewSchema)

  }

  /**
   * Creates a new relation for a cassandra table given table, keyspace, cluster, push_down and schema
   * as parameters. It saves the data to the Cassandra table depends on [[SaveMode]]
   */
  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {

    val (tableIdent, options) = parseParameters(parameters)
    val table = sqlContext.createCassandraSourceRelation(tableIdent, options)

    mode match {
      case Append => table.insert(data, overwrite = false)
      case Overwrite => table.insert(data, overwrite = true)
      case ErrorIfExists =>
        if (table.buildScan().isEmpty()) {
          table.insert(data, overwrite = false)
        } else {
          throw new UnsupportedOperationException("Writing to a none-empty Cassandra Table is not allowed.")
        }
      case Ignore =>
        if (table.buildScan().isEmpty()) {
          table.insert(data, overwrite = false)
        }
    }

    sqlContext.createCassandraSourceRelation(tableIdent, options)
  }
}

/** Store data source options */
case class CassandraDataSourceOptions(schema: Option[StructType] = None, pushdown: Boolean = true)

/** It's not a companion object because it throws exception when create a new instance by reflection. */
object CassandraDefaultSource {
  val CassandraDataSourceTableNameProperty = "c_table"
  val CassandraDataSourceKeyspaceNameProperty = "keyspace"
  val CassandraDataSourceClusterNameProperty = "cluster"
  val CassandraDataSourceUserDefinedSchemaNameProperty = "schema"
  val CassandraDataSourcePushdownEnableProperty = "push_down"
  val CassandraDataSourceProviderName = CassandraDefaultSource.getClass.getPackage.getName
  val CassandraDataSourceProviderFullName = CassandraDefaultSource.getClass.getPackage.getName + ".DefaultSource"


  /** Parse parameters into CassandraDataSourceOptions and TableIdent object */
  def parseParameters(parameters: Map[String, String]) : (TableIdent, CassandraDataSourceOptions) = {
    val tableName = parameters(CassandraDataSourceTableNameProperty)
    val keyspaceName = parameters(CassandraDataSourceKeyspaceNameProperty)
    val clusterName = parameters.get(CassandraDataSourceClusterNameProperty)
    val schema = parameters.get(CassandraDataSourceUserDefinedSchemaNameProperty)
      .map(DataType.fromJson).map(_.asInstanceOf[StructType])
    val pushdown : Boolean = parameters.getOrElse(CassandraDataSourcePushdownEnableProperty, "true").toBoolean

    (TableIdent(tableName, keyspaceName, clusterName), CassandraDataSourceOptions(schema, pushdown))
  }
}

