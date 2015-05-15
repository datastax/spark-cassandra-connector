package org.apache.spark.sql.cassandra

import com.datastax.spark.connector
import com.datastax.spark.connector.cql.ColumnDef
import com.datastax.spark.connector.types.FieldDef
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{types => catalystTypes}

/** Convert Cassandra data type to Catalyst data type */
object DataTypeConverter {

  private[cassandra] val primitiveTypeMap = Map[connector.types.ColumnType[_], catalystTypes.DataType](
    connector.types.TextType       -> catalystTypes.StringType,
    connector.types.AsciiType      -> catalystTypes.StringType,
    connector.types.VarCharType    -> catalystTypes.StringType,

    connector.types.BooleanType    -> catalystTypes.BooleanType,

    connector.types.IntType        -> catalystTypes.IntegerType,
    connector.types.BigIntType     -> catalystTypes.LongType,
    connector.types.CounterType    -> catalystTypes.LongType,
    connector.types.FloatType      -> catalystTypes.FloatType,
    connector.types.DoubleType     -> catalystTypes.DoubleType,

    connector.types.VarIntType     -> catalystTypes.DecimalType(), // no native arbitrary-size integer type
    connector.types.DecimalType    -> catalystTypes.DecimalType(),

    connector.types.TimestampType  -> catalystTypes.TimestampType,
    connector.types.InetType       -> catalystTypes.StringType,
    connector.types.UUIDType       -> catalystTypes.StringType,
    connector.types.TimeUUIDType   -> catalystTypes.StringType,
    connector.types.BlobType       -> catalystTypes.BinaryType
  )

  /** Convert Cassandra data type to Catalyst data type */
  def catalystDataType(cassandraType: connector.types.ColumnType[_], nullable: Boolean): catalystTypes.DataType = {

    def catalystStructField(field: FieldDef): StructField =
      StructField(field.fieldName, catalystDataType(field.fieldType, nullable = true), nullable = true)

    cassandraType match {
      case connector.types.SetType(et)                => catalystTypes.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.ListType(et)               => catalystTypes.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.MapType(kt, vt)            => catalystTypes.MapType(primitiveTypeMap(kt), primitiveTypeMap(vt), nullable)
      case connector.types.UserDefinedType(_, fields) => catalystTypes.StructType(fields.map(catalystStructField))
      case _                                          => primitiveTypeMap(cassandraType)
    }
  }

  /** Create a Catalyst StructField from a Cassandra Column */
  def toStructField(column: ColumnDef): StructField =
    StructField(column.columnName,catalystDataType(column.columnType, nullable = true))

}
