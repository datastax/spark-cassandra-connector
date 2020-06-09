package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.TableRef
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.datasource.{CassandraCatalog, CassandraTable}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider, StreamSinkProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 *  A Pointer to the DatasourceV2 Implementation of The Cassandra Source
 *
 *      CREATE TEMPORARY TABLE tmpTable
 *      USING org.apache.spark.sql.cassandra
 *      OPTIONS (
 *       table "table",
 *       keyspace "keyspace",
 *       cluster "test_cluster",
 *       pushdown "true",
 *       spark.cassandra.input.fetch.sizeInRows "10",
 *       spark.cassandra.output.consistency.level "ONE",
 *       spark.cassandra.connection.timeoutMS "1000"
 *      )
 */
class DefaultSource() extends TableProvider with DataSourceRegister {

  override def getTable(schema: StructType, partitioning: Array[Transform], properties:java.util.Map[String,String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

  override def shortName(): String = "cassandra"

  def getTable(options: CaseInsensitiveStringMap): CassandraTable = {
    val session = SparkSession.active
    val sparkConf = session.sparkContext.getConf
    val scalaOptions = options.asScala

    val keyspace = scalaOptions.getOrElse("keyspace",
      throw new IllegalArgumentException("No keyspace specified in options of Cassandra Source"))

    val table = scalaOptions.getOrElse("table",
      throw new IllegalArgumentException("No table specified in options of Cassandra Source"))

    val cluster = scalaOptions.getOrElse("cluster", "default")
    val connectorConf =  consolidateConfs(
      sparkConf,
      session.conf.getAll,
      cluster,
      keyspace,
      scalaOptions.toMap)
    val connector = CassandraConnector(connectorConf)

    CassandraTable(
      session,
      options,
      connector,
      cluster,
      CassandraCatalog.getTableMetaData(connector, Identifier.of(Array(keyspace), table)))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(options).schema()
  }

}

/** Store data source options */
case class CassandraSourceOptions(pushdown: Boolean = true, confirmTruncate: Boolean = false, cassandraConfs: Map[String, String] = Map.empty)

object DefaultSource {
  val CassandraDataSourceTableNameProperty = "table"
  val CassandraDataSourceKeyspaceNameProperty = "keyspace"
  val CassandraDataSourceClusterNameProperty = "cluster"
  val CassandraDataSourceUserDefinedSchemaNameProperty = "schema"
  val CassandraDataSourcePushdownEnableProperty = "pushdown"
  val CassandraDataSourceConfirmTruncateProperty = "confirm.truncate"
  val CassandraDataSourceProviderPackageName = "org.apache.spark.sql.cassandra"
  val CassandraDataSourceProviderClassName = CassandraDataSourceProviderPackageName + ".DefaultSource"


  /** Parse parameters into CassandraDataSourceOptions and TableRef object */
  def TableRefAndOptions(parameters: Map[String, String]) : (TableRef, CassandraSourceOptions) = {
    val tableName = parameters(CassandraDataSourceTableNameProperty)
    val keyspaceName = parameters(CassandraDataSourceKeyspaceNameProperty)
    val clusterName = parameters.get(CassandraDataSourceClusterNameProperty)
    val pushdown : Boolean = parameters.getOrElse(CassandraDataSourcePushdownEnableProperty, "true").toBoolean
    val confirmTruncate : Boolean = parameters.getOrElse(CassandraDataSourceConfirmTruncateProperty, "false").toBoolean
    val cassandraConfs = parameters

    (TableRef(tableName, keyspaceName, clusterName), CassandraSourceOptions(pushdown, confirmTruncate, cassandraConfs))
  }

  /** Check whether the provider is Cassandra datasource or not */
  def cassandraSource(provider: String) : Boolean = {
    provider == CassandraDataSourceProviderPackageName || provider == CassandraDataSourceProviderClassName
  }
}
