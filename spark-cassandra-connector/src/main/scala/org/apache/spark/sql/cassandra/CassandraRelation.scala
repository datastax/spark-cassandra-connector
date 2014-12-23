package org.apache.spark.sql.cassandra

import com.datastax.spark.connector
import com.datastax.spark.connector.cql.{ColumnDef, TableDef}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Statistics, LeafNode}
import org.apache.spark.sql.catalyst

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

  def columnToAttribute(column: ColumnDef): AttributeReference = {
    // Since data can be dumped in randomly with no validation, everything is nullable.
    val catalystType = ColumnDataType.catalystDataType(column.columnType, nullable = true)
    val qualifiers = tableDef.tableName +: alias.toSeq
    new AttributeReference(column.columnName, catalystType, nullable = true)(qualifiers = qualifiers)
  }

  override def output: Seq[Attribute] = projectAttributes

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      BigInt(cc.conf.getLong(keyspaceName + "." + tableName + ".size.in.bytes", cc.defaultSizeInBytes))
    }
  )

  def tableName = tableDef.tableName
}

object ColumnDataType {

  private val primitiveTypeMap = Map[connector.types.ColumnType[_], catalyst.types.DataType](

    connector.types.TextType       -> catalyst.types.StringType,
    connector.types.AsciiType      -> catalyst.types.StringType,
    connector.types.VarCharType    -> catalyst.types.StringType,

    connector.types.BooleanType    -> catalyst.types.BooleanType,

    connector.types.IntType        -> catalyst.types.IntegerType,
    connector.types.BigIntType     -> catalyst.types.LongType,
    connector.types.CounterType    -> catalyst.types.LongType,
    connector.types.FloatType      -> catalyst.types.FloatType,
    connector.types.DoubleType     -> catalyst.types.DoubleType,
  
    connector.types.VarIntType     -> catalyst.types.DecimalType(), // no native arbitrary-size integer type
    connector.types.DecimalType    -> catalyst.types.DecimalType(),

    connector.types.TimestampType  -> catalyst.types.TimestampType,
    connector.types.InetType       -> catalyst.types.StringType, 
    connector.types.UUIDType       -> catalyst.types.StringType,
    connector.types.TimeUUIDType   -> catalyst.types.StringType,
    connector.types.BlobType       -> catalyst.types.ByteType,
  
    // TODO: This mapping is useless, it is here only to avoid lookup failure if a table contains a UDT column. 
    // It is not possible to read UDT columns in SparkSQL now. 
    connector.types.UserDefinedTypeStub -> catalyst.types.StructType(Seq.empty)
  )

  def catalystDataType(cassandraType: connector.types.ColumnType[_], nullable: Boolean): catalyst.types.DataType = {
    cassandraType match {
      case connector.types.SetType(et)      => catalyst.types.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.ListType(et)     => catalyst.types.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.MapType(kt, vt)  => catalyst.types.MapType(primitiveTypeMap(kt), primitiveTypeMap(vt), nullable)
      case _                                => primitiveTypeMap(cassandraType)
    }
  }
}
