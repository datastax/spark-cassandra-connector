package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{RowReader, RowReaderFactory}
import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata, ColumnRef}
import org.apache.spark.sql.cassandra.CassandraSQLRow.toUnsafeSqlType
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.StructType
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

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    *
    * @param row         row fetched from Cassandra
    * @param rowMetaData column names and codec available in the `row`*/
  override def read(row: Row, rowMetaData: CassandraRowMetadata): UnsafeRow = {
    val data = CassandraRow.dataFromJavaDriverRow(row, rowMetaData)
    val converterOutput = converter
      .apply(SparkRow(data.map(toUnsafeSqlType): _*))
      .asInstanceOf[InternalRow]

    projection.apply(converterOutput)
  }

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  override def neededColumns: Option[Seq[ColumnRef]] = None
}