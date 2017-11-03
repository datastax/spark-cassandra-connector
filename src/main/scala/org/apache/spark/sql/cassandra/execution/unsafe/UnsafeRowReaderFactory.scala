package org.apache.spark.sql.cassandra.execution.unsafe

import com.datastax.driver.core.Row
import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata, ColumnRef}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{RowReader, RowReaderFactory}
import org.apache.spark.sql.cassandra.CassandraSQLRow.toUnsafeSqlType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StructType

class UnsafeRowReaderFactory(schema: StructType) extends RowReaderFactory[UnsafeRow] {

  override def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]):
  RowReader[UnsafeRow] = new UnsafeRowReader(schema)

  override def targetClass: Class[UnsafeRow] = classOf[UnsafeRow]
}

class UnsafeRowReader(schema: StructType)
  extends RowReader[UnsafeRow]{

  @transient lazy val projection = UnsafeProjection.create(schema)

  /** Reads column values from low-level `Row` and turns them into higher level representation.
    *
    * @param row         row fetched from Cassandra
    * @param rowMetaData column names and codec available in the `row`*/
  override def read(row: Row, rowMetaData: CassandraRowMetadata): UnsafeRow = {
    val data = CassandraRow.dataFromJavaDriverRow(row, rowMetaData)
    projection.apply(InternalRow(data.map(toUnsafeSqlType): _*))
  }

  /** List of columns this `RowReader` is going to read.
    * Useful to avoid fetching the columns that are not needed. */
  override def neededColumns: Option[Seq[ColumnRef]] = None
}