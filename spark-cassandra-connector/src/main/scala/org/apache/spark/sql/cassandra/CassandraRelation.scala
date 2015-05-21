package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.{ColumnDef, TableDef}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LeafNode}

private[cassandra] case class CassandraRelation
    (tableDef: TableDef, alias: Option[String], cluster: Option[String] = None)
    (@transient val cc: CassandraSQLContext)
  extends LeafNode {

  val keyspaceName          = tableDef.keyspaceName
  val regularColumns        = tableDef.regularColumns.toList.map(columnToAttribute)
  val indexedColumns        = tableDef.regularColumns.filter(_.isIndexedColumn).map(columnToAttribute)
  val partitionColumns      = tableDef.partitionKey.map(columnToAttribute)
  val clusterColumns        = tableDef.clusteringColumns.map(columnToAttribute)
  val allColumns            = tableDef.regularColumns ++ tableDef.partitionKey ++ tableDef.clusteringColumns
  val columnNameByLowercase = allColumns.map(c => (c.columnName.toLowerCase, c.columnName)).toMap
  var projectAttributes     = tableDef.columns.map(columnToAttribute)

  def columnToAttribute(column: ColumnDef): AttributeReference = {
    // Since data can be dumped in randomly with no validation, everything is nullable.
    val catalystType = DataTypeConverter.catalystDataType(column.columnType, nullable = true)
    val qualifiers = tableDef.tableName +: alias.toSeq
    new AttributeReference(column.columnName, catalystType, nullable = true)(qualifiers = qualifiers)
  }

  override def output: Seq[Attribute] = projectAttributes

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      BigInt(cc.sparkConf.getLong(keyspaceName + "." + tableName + ".size.in.bytes", cc.conf.defaultSizeInBytes))
    }
  )

  def tableName = tableDef.tableName
}

