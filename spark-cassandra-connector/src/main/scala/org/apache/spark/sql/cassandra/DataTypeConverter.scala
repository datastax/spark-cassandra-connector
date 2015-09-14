package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.types.UDTFieldDef
import org.apache.spark.Logging
import org.apache.spark.sql.cassandra.types.{UUIDType, InetAddressType}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{types => catalystTypes}

import com.datastax.spark.connector
import com.datastax.spark.connector.cql.ColumnDef

/** Convert Cassandra data type to Catalyst data type */
object DataTypeConverter extends Logging {

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

    connector.types.VarIntType     -> catalystTypes.DecimalType(38, 0), // no native arbitrary-size integer type
    connector.types.DecimalType    -> catalystTypes.DecimalType(38, 18),

    connector.types.TimestampType  -> catalystTypes.TimestampType,
    connector.types.InetType       -> InetAddressType,
    connector.types.UUIDType       -> UUIDType,
    connector.types.TimeUUIDType   -> UUIDType,
    connector.types.BlobType       -> catalystTypes.BinaryType
  )

  /** Convert Cassandra data type to Catalyst data type */
  def catalystDataType(cassandraType: connector.types.ColumnType[_], nullable: Boolean): catalystTypes.DataType = {

    def catalystStructField(field: UDTFieldDef): StructField =
      StructField(field.columnName, catalystDataType(field.columnType, nullable = true), nullable = true)

    cassandraType match {
      case connector.types.SetType(et)                => catalystTypes.ArrayType(catalystDataType(et, nullable), nullable)
      case connector.types.ListType(et)               => catalystTypes.ArrayType(catalystDataType(et, nullable), nullable)
      case connector.types.MapType(kt, vt)            => catalystTypes.MapType(catalystDataType(kt, nullable), catalystDataType(vt, nullable), nullable)
      case connector.types.UserDefinedType(_, fields) => catalystTypes.StructType(fields.map(catalystStructField))
      case connector.types.VarIntType                 =>
        logWarning("VarIntType is mapped to catalystTypes.DecimalType with unlimited values.")
        primitiveTypeMap(cassandraType)
      case _                                          => primitiveTypeMap(cassandraType)
    }
  }

  /** Create a Catalyst StructField from a Cassandra Column */
  def toStructField(column: ColumnDef): StructField =
    StructField(column.columnName,catalystDataType(column.columnType, nullable = true))

}
