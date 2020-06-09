package com.datastax.spark.connector.datasource

import java.util

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class CassandraTable(
  session: SparkSession,
  catalogConf: CaseInsensitiveStringMap,
  connector: CassandraConnector,
  catalogName: String,
  metadata: TableMetadata,
  optionalSchema: Option[StructType] = None) //Used for adding metadata references

  extends Table
    with SupportsRead
    with SupportsWrite {

  val tableDef = tableFromCassandra(connector, metadata.getKeyspace.asInternal(), name())

  override def schema(): StructType = optionalSchema
    .getOrElse(CassandraSourceUtil.toStructType(metadata))

  override def partitioning(): Array[Transform] = {
    metadata.getPartitionKey.asScala
      .map(_.getName.asInternal())
      .map(Expressions.identity)
      .toArray
  }

  override def properties(): util.Map[String, String] = {
    val clusteringKey = metadata.getClusteringColumns().asScala
      .map { case (col, ord) => s"${col.getName.asInternal}.$ord" }
      .mkString("[", ",", "]")
    (
      Map("clustering_key" -> clusteringKey)
        ++ metadata.getOptions.asScala.map { case (key, value) => key.asInternal() -> value.toString }
      ).asJava
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.TRUNCATE).asJava


  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val combinedOptions = (catalogConf.asScala ++ options.asScala).asJava
    CassandraScanBuilder(session, tableDef, catalogName, new CaseInsensitiveStringMap(combinedOptions))
  }

  override def name(): String = metadata.getName.asInternal()

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val combinedOptions = (catalogConf.asScala ++ info.options.asScala).asJava
    CassandraWriteBuilder(session, tableDef, catalogName, new CaseInsensitiveStringMap(combinedOptions), info.schema)
  }
}

