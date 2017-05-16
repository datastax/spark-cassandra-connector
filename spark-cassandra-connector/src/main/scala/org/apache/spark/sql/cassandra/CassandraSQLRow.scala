package org.apache.spark.sql.cassandra

import java.math.BigInteger
import java.net.InetAddress
import java.sql.Timestamp
import java.util.{Date, UUID}

import com.datastax.driver.core.Row
import com.datastax.spark.connector.rdd.reader.{RowReader, ThisRowReaderAsFactory}
import com.datastax.spark.connector.types.{ColumnType, TypeConverter}
import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata, GettableData, TupleValue, UDTValue}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.unsafe.types.UTF8String

final class CassandraSQLRow(val metaData: CassandraRowMetadata, val columnValues: IndexedSeq[AnyRef])
  extends GettableData with SparkRow with Serializable {

  protected def fieldNames = metaData

  private[spark] def this() = this(null, null) // required by Kryo for deserialization :(

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  private def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(columnValues(index))

  override def apply(i: Int) = columnValues(i)
  override def copy() = this // immutable
  override def size = super.size

  override def getDouble(i: Int) = get[Double](i)
  override def getFloat(i: Int) = get[Float](i)
  override def getLong(i: Int) = get[Long](i)
  override def getByte(i: Int) = get[Byte](i)
  override def getBoolean(i: Int) = get[Boolean](i)
  override def getShort(i: Int) = get[Short](i)
  override def getInt(i: Int) = get[Int](i)
  override def getString(i: Int) = get[String](i)
  override def get(i: Int) = get[Any](i)

  override def isNullAt(i: Int): Boolean = super[GettableData].isNullAt(i)

  override def toSeq: Seq[Any] = columnValues
}


object CassandraSQLRow {

  def fromJavaDriverRow(row: Row, metaData:CassandraRowMetadata): CassandraSQLRow = {
    val data = CassandraRow.dataFromJavaDriverRow(row, metaData)
    new CassandraSQLRow(metaData, data.map(toSparkSqlType))
  }

  implicit object CassandraSQLRowReader extends RowReader[CassandraSQLRow] with ThisRowReaderAsFactory[CassandraSQLRow] {

    override def read(row: Row, metaData:CassandraRowMetadata): CassandraSQLRow =
      fromJavaDriverRow(row, metaData)

    override def neededColumns = None
    override def targetClass = classOf[CassandraSQLRow]
  }

  private lazy val customCatalystDataTypeConverter: PartialFunction[Any, AnyRef] = {
    ColumnType.customDriverConverter
      .flatMap(clazz => Some(clazz.catalystDataTypeConverter))
      .getOrElse(PartialFunction.empty)
  }

  def toSparkSqlType(value: Any): AnyRef = {
    val sparkSqlType: PartialFunction[Any, AnyRef] = customCatalystDataTypeConverter orElse {
      case date: Date => new Timestamp(date.getTime)
      case localDate: org.joda.time.LocalDate =>
        new java.sql.Date(localDate.toDateTimeAtStartOfDay().getMillis)
      case localDate: java.time.LocalDate => java.sql.Date.valueOf(localDate)
      case localTime: java.time.LocalTime => localTime.toNanoOfDay.asInstanceOf[AnyRef]
      case duration: java.time.Duration => duration.toMillis.asInstanceOf[AnyRef]
      case instant: java.time.Instant => new java.sql.Timestamp(instant.toEpochMilli)
      case str: String => UTF8String.fromString(str)
      case bigInteger: BigInteger => Decimal(bigInteger.toString)
      case inetAddress: InetAddress => UTF8String.fromString(inetAddress.getHostAddress)
      case uuid: UUID => UTF8String.fromString(uuid.toString)
      case set: Set[_] => set.map(toSparkSqlType).toSeq
      case list: List[_] => list.map(toSparkSqlType)
      case map: Map[_, _] => map map { case(k, v) => (toSparkSqlType(k), toSparkSqlType(v))}
      case udt: UDTValue => UDTValue(udt.columnNames, udt.columnValues.map(toSparkSqlType))
      case tupleValue: TupleValue => TupleValue(tupleValue.values.map(toSparkSqlType): _*)
      case _ => value.asInstanceOf[AnyRef]
    }
    sparkSqlType(value)
  }

  val empty = new CassandraSQLRow(null, IndexedSeq.empty)
}

