package org.apache.spark.sql.cassandra

import com.datastax.spark.connector
import com.datastax.spark.connector.cql.{ColumnDef, TableDef}
import com.datastax.spark.connector.types.UDTFieldDef

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LeafNode}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{catalyst, types}

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
    val catalystType = ColumnDataType.catalystDataType(column.columnType, nullable = true)
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

object ColumnDataType {

  private val primitiveTypeMap = Map[connector.types.ColumnType[_], types.DataType](
    connector.types.TextType       -> types.StringType,
    connector.types.AsciiType      -> types.StringType,
    connector.types.VarCharType    -> types.StringType,

    connector.types.BooleanType    -> types.BooleanType,

    connector.types.IntType        -> types.IntegerType,
    connector.types.BigIntType     -> types.LongType,
    connector.types.CounterType    -> types.LongType,
    connector.types.FloatType      -> types.FloatType,
    connector.types.DoubleType     -> types.DoubleType,
  
    connector.types.VarIntType     -> types.DecimalType(), // no native arbitrary-size integer type
    connector.types.DecimalType    -> types.DecimalType(),

    connector.types.TimestampType  -> types.TimestampType,
    connector.types.InetType       -> types.StringType,
    connector.types.UUIDType       -> types.StringType,
    connector.types.TimeUUIDType   -> types.StringType,
    connector.types.BlobType       -> types.ByteType
  )

  def catalystDataType(cassandraType: connector.types.ColumnType[_], nullable: Boolean): types.DataType = {

    def catalystStructField(field: UDTFieldDef): StructField =
      StructField(field.columnName, catalystDataType(field.columnType, nullable = true), nullable = true)

    cassandraType match {
      case connector.types.SetType(et)                => types.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.ListType(et)               => types.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.MapType(kt, vt)            => types.MapType(primitiveTypeMap(kt), primitiveTypeMap(vt), nullable)
      case connector.types.UserDefinedType(_, fields) => types.StructType(fields.map(catalystStructField))
      case _                                          => primitiveTypeMap(cassandraType)
    }
  }
}
