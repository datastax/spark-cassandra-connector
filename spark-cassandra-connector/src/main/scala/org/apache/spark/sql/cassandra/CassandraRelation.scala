package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.{ColumnDef, TableDef}
import org.apache.hadoop.hive.ql.stats.StatsSetupConst
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.types.DataType

private[cassandra] case class CassandraRelation
  (tableDef: TableDef, alias: Option[String])(@transient cc: CassandraSQLContext)
  extends LeafNode {

  val keyspaceName          = tableDef.keyspaceName
  val regularColumns        = tableDef.regularColumns.toList.map(columnToAttribute)
  val indexedColumns        = tableDef.regularColumns.filter(_.isIndexedColumn).map(columnToAttribute)
  val partitionColumns      = tableDef.partitionKey.map(columnToAttribute)
  val clusterColumns        = tableDef.clusteringColumns.map(columnToAttribute)
  val allColumns            = tableDef.regularColumns ++ tableDef.partitionKey ++ tableDef.clusteringColumns
  val columnNameByLowercase = allColumns.map(c => (c.columnName.toLowerCase, c.columnName)).toMap
  var projectAttributes     = tableDef.allColumns.map(columnToAttribute)

  def columnToAttribute(column: ColumnDef): AttributeReference = new AttributeReference(
      column.columnName,
      ColumnDataType.scalaDataType(column.columnType.scalaTypeName, true),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = tableDef.tableName +: alias.toSeq)

  override def output: Seq[Attribute] = projectAttributes

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      BigInt(cc.conf.getLong(keyspaceName + "." + tableName + ".size.in.bytes", cc.defaultSizeInBytes))
    }
  )
  def tableName = tableDef.tableName
}

object ColumnDataType {

  implicit class Regex(sc: StringContext) {
    def regex = new scala.util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  private val primitiveTypeMap = Map[String, String](
    "String"               -> "StringType",
    "Int"                  -> "IntegerType",
    "Long"                 -> "LongType",
    "Float"                -> "FloatType",
    "Double"               -> "DoubleType",
    "Boolean"              -> "BooleanType",
    "BigInt"               -> "LongType",
    "BigDecimal"           -> "DecimalType",
    "java.util.Date"       -> "TimestampType",
    "java.net.InetAddress" -> "StringType",
    "java.util.UUID"       -> "StringType",
    "java.nio.ByteBuffer"  -> "ByteType"
  )

  def scalaDataType(scalaType: String, containNull: Boolean): DataType = {
    scalaType match {
      case regex"Set\[(\w+)$dt\]"              => DataType("ArrayType(" + primitiveTypeMap(dt) + ", " + containNull + ")")
      case regex"Vector\[(\w+)$dt\]"           => DataType("ArrayType(" + primitiveTypeMap(dt) + ", " + containNull + ")")
      case regex"Map\[(\w+)$key,(\w+)$value\]" => DataType("MapType(" + primitiveTypeMap(key) + "," + primitiveTypeMap(value) + ", " + containNull + ")")
      case _                                   => DataType(primitiveTypeMap(scalaType))
    }
  }
}
