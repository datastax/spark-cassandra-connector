package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{RowReader, RowReaderFactory}
import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata, ColumnRef, UDTValue}
import org.apache.spark.sql.cassandra.CassandraSQLRow.toUnsafeSqlType
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.{Row => SparkRow}

class UnsafeRowReaderFactory(schema: StructType) extends RowReaderFactory[UnsafeRow] {

  override def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]):
  RowReader[UnsafeRow] = new UnsafeRowReader(schema)

  override def targetClass: Class[UnsafeRow] = classOf[UnsafeRow]
}

class UnsafeRowReader(schema: StructType)
  extends RowReader[UnsafeRow] {

  @transient private lazy val projection = UnsafeProjection.create(schema)
  private val converter = CatalystTypeConverters.createToCatalystConverter(schema)

  val projectionDecoder = UdtProjectionDecoder.build(schema)

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    *
    * @param row         row fetched from Cassandra
    * @param rowMetaData column names and codec available in the `row`*/
  override def read(row: Row, rowMetaData: CassandraRowMetadata): UnsafeRow = {
    val data = CassandraRow.dataFromJavaDriverRow(row, rowMetaData)
    val sparkRow = SparkRow(data.map(toUnsafeSqlType): _*)
    val projectionDecoded = projectionDecoder(sparkRow)
    val converterOutput = converter
      .apply(projectionDecoded)
      .asInstanceOf[InternalRow]

    projection.apply(converterOutput)
  }

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  override def neededColumns: Option[Seq[ColumnRef]] = None
}

/**
 * Helper for decoding sub selections of Cassandra UDTs.
 * (Cassandra always responds with the full UDT, however Spark exepects only selected fields).
 *
 * The conversion is done on the level of Scala Data types, after conversion from Cassandra
 * and before conversion to Catalyst.
 * */
object UdtProjectionDecoder {

  /** Build a decoder for UDT Projections. */
  def build(schema: StructType): SparkRow => SparkRow = {
    if (hasProjections(schema)) {
      buildRootDecoder(schema)
    } else {
      // No need to traverse the whole tree
      identity
    }
  }

  private def buildRootDecoder(schema: StructType): SparkRow => SparkRow = {
    val childEncoders = schema.fields.map(field => buildDataTypeDecoder(field.dataType))
    row => {
      val updated = Array.tabulate[Any](childEncoders.length) { idx =>
        childEncoders(idx)(row.get(idx))
      }
      SparkRow(updated: _*)
    }
  }

  def buildDataTypeDecoder(dataType: DataType): Any => Any = {
    dataType match {
      case s: StructType => structTypeDecoder(s)
      case a: ArrayType => arrayTypeDecoder(a)
      case m: MapType => mapTypeDecoder(m)
      case _ =>
        identity
    }
  }

  private def structTypeDecoder(structType: StructType): Any => Any = {
    val childDecoders = structType.fields.map(field => buildDataTypeDecoder(field.dataType)).toIndexedSeq
    input => {
      input match {
        case null => null
        case udt: UDTValue =>
          val selectedValues = structType.fields.zipWithIndex.map { case (field, idx) =>
            val originalIndex = udt.indexOf(field.name)
            val decoded = childDecoders(idx)(udt.columnValues(originalIndex))
            decoded.asInstanceOf[AnyRef]
          }
          UDTValue.apply(structType.fieldNames.toIndexedSeq, selectedValues.toIndexedSeq)
        case other =>
          // ??
          other
      }
    }
  }

  private def arrayTypeDecoder(arrayType: ArrayType): Any => Any = {
    val keyDecoder = buildDataTypeDecoder(arrayType.elementType)
    input => {
      input match {
        case null => null
        case s: Seq[_] =>
          s.map(keyDecoder)
        case other =>
          // ??
          other
      }
    }
  }

  private def mapTypeDecoder(mapType: MapType): Any => Any = {
    val keyDecoder = buildDataTypeDecoder(mapType.keyType)
    val valueDecoder = buildDataTypeDecoder(mapType.valueType)
    input => {
      input match {
        case null => null
        case m: Map[_, _] =>
          m.toSeq.map { case (k, v) =>
            keyDecoder(k) -> valueDecoder(v)
          }.toMap
        case other =>
          // ??
          other
      }
    }
  }

  private def hasProjections(schema: StructType): Boolean = {
    schema.fields.exists { structField =>
      hasStructTypes(structField.dataType)
    }
  }

  private def hasStructTypes(dataType: DataType): Boolean = {
    dataType match {
      case _: StructType => true
      case a: ArrayType => hasStructTypes(a.elementType)
      case m: MapType => hasStructTypes(m.keyType) || hasStructTypes(m.valueType)
      case _ => false
    }
  }
}