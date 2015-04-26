package org.apache.spark.sql.cassandra

import com.datastax.spark.connector
import com.datastax.spark.connector.cql.ColumnDef
import com.datastax.spark.connector.types.FieldDef
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructField

/** Convert Cassandra data type to Catalyst data type */
object DataTypeConverter {

  private[cassandra] val primitiveTypeMap = Map[connector.types.ColumnType[_], types.DataType](
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
    connector.types.BlobType       -> types.BinaryType
  )

  /** Convert Cassandra data type to Catalyst data type */
  def catalystDataType(cassandraType: connector.types.ColumnType[_], nullable: Boolean): types.DataType = {

    def catalystStructField(field: FieldDef): StructField =
      StructField(field.fieldName, catalystDataType(field.fieldType, nullable = true), nullable = true)

    cassandraType match {
      case connector.types.SetType(et)                => types.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.ListType(et)               => types.ArrayType(primitiveTypeMap(et), nullable)
      case connector.types.MapType(kt, vt)            => types.MapType(primitiveTypeMap(kt), primitiveTypeMap(vt), nullable)
      case connector.types.UserDefinedType(_, fields) => types.StructType(fields.map(catalystStructField))
      case _                                          => primitiveTypeMap(cassandraType)
    }
  }

  /** Create a Catalyst StructField from a Cassandra Column */
  def toStructField(column: ColumnDef): StructField =
    StructField(column.columnName,catalystDataType(column.columnType, nullable = true))

}
