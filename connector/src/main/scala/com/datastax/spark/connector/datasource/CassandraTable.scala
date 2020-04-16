package com.datastax.spark.connector.datasource

import java.util

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class CassandraTable(val metadata: TableMetadata) extends Table {
  override def name(): String = metadata.getName.asInternal()

  override def schema(): StructType = CassandraSourceUtil.toStructType(metadata)

  override def partitioning(): Array[Transform] = {
    metadata.getPartitionKey.asScala
      .map(_.getName.asInternal())
      .map(Expressions.identity)
      .toArray
  }

  override def properties(): util.Map[String, String] = {
    val clusteringKey = metadata.getClusteringColumns().asScala
      .map{case (col, ord) => s"${col.getName.asInternal}.$ord"}
      .mkString("[", ",", "]")
    (
      Map("clustering_key" -> clusteringKey)
        ++ metadata.getOptions.asScala.map{case (key, value) => key.asInternal() -> value.toString}
      ).asJava
  }

  override def capabilities(): util.Set[TableCapability] = Set.empty[TableCapability].asJava

}

object CassandraTable {
  def apply(metadata: TableMetadata): CassandraTable = {
    new CassandraTable(metadata)
  }
}
