package org.apache.spark.sql.cassandra

import java.math.BigInteger
import java.net.InetAddress
import java.sql.Timestamp
import java.util.{Date, UUID}

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{RowReader, RowReaderFactory}
import com.datastax.spark.connector.{CassandraRowMetadata, ColumnRef, GettableData, TupleValue, UDTValue}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.types.{Decimal, StructType}
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.unsafe.types.UTF8String

final class CassandraSQLRow(val cassandraRow: Row, val metaData: CassandraRowMetadata)
  extends BaseGenericInternalRow with Serializable {

  protected def fieldNames = metaData

  private[spark] def this() = this(null, null) // required by Kryo for deserialization :(

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */

  override protected def genericGet(ordinal: Int): Any =
  CassandraSQLRow.toSparkSqlType(GettableData.get(cassandraRow, ordinal, metaData.codecs(ordinal)))

  override def numFields: Int = metaData.columnNames.length

  override def copy(): InternalRow = this //immutable row
}

object CassandraSQLRow {

  def fromJavaDriverRow(row: Row, metaData: CassandraRowMetadata): CassandraSQLRow = {
    new CassandraSQLRow(row, metaData)
  }

  case class CassandraSQLRowReader(resultSchema: StructType) extends RowReader[InternalRow] {
    @transient
    lazy val unsafeProjection = UnsafeProjection.create(resultSchema)

    override def read(row: Row, metaData: CassandraRowMetadata): InternalRow =
      unsafeProjection(fromJavaDriverRow(row, metaData))

    override def neededColumns = None
  }

  case class CassandraSQLRowReaderFactory(resultSchema: StructType) extends RowReaderFactory[InternalRow] {
    override def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[InternalRow] = {
      CassandraSQLRowReader(resultSchema)
    }

    override def targetClass = classOf[InternalRow]
  }

  private def toSparkSqlType(value: Any): AnyRef = {
    value match {
      case date: Date => new Timestamp(date.getTime)
      case localDate: org.joda.time.LocalDate =>
        new java.sql.Date(localDate.toDateTimeAtStartOfDay().getMillis)
      case str: String => UTF8String.fromString(str)
      case bigInteger: BigInteger => Decimal(bigInteger.toString)
      case inetAddress: InetAddress => UTF8String.fromString(inetAddress.getHostAddress)
      case uuid: UUID => UTF8String.fromString(uuid.toString)
      case set: Set[_] => set.map(toSparkSqlType).toSeq
      case list: List[_] => list.map(toSparkSqlType)
      case map: Map[_, _] => map map { case (k, v) => (toSparkSqlType(k), toSparkSqlType(v)) }
      case udt: UDTValue => UDTValue(udt.columnNames, udt.columnValues.map(toSparkSqlType))
      case tupleValue: TupleValue => TupleValue(tupleValue.values.map(toSparkSqlType): _*)
      case _ => value.asInstanceOf[AnyRef]
    }
  }
}

