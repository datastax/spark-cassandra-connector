package org.apache.spark.sql.cassandra

import java.net.InetAddress
import java.sql.Timestamp
import java.util.{UUID, Date}
import java.math.BigInteger

import com.datastax.driver.core.Row
import com.datastax.driver.core.LocalDate
import com.datastax.spark.connector.{TupleValue, UDTValue, GettableData}
import com.datastax.spark.connector.rdd.reader.{ThisRowReaderAsFactory, RowReader}
import com.datastax.spark.connector.types.TypeConverter

import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.Decimal

final class CassandraSQLRow(val columnNames: IndexedSeq[String], val columnValues: IndexedSeq[AnyRef])
  extends GettableData with SparkRow with Serializable {

  protected def fieldNames = columnNames

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

  /** SparkSQL assumes all incoming Timestamps will be shifted by the
    * local TimeZone, if we do not anticipate this our LocalDate objects will
    * go back in time.
    */
  lazy val defaultTimeZone = java.util.TimeZone.getDefault
  def subtractTimeZoneOffset( millis: Long ) = millis - defaultTimeZone.getOffset(millis)

  def fromJavaDriverRow(row: Row, columnNames: Array[String]): CassandraSQLRow = {
    val data = new Array[Object](columnNames.length)
    for (i <- columnNames.indices) {
      data(i) = GettableData.get(row, i)
      data.update(i, toSparkSqlType(data(i)))
    }
    new CassandraSQLRow(columnNames, data)
  }

  implicit object CassandraSQLRowReader extends RowReader[CassandraSQLRow] with ThisRowReaderAsFactory[CassandraSQLRow] {

    override def read(row: Row, columnNames: Array[String]): CassandraSQLRow =
      fromJavaDriverRow(row, columnNames)

    override def neededColumns = None
    override def targetClass = classOf[CassandraSQLRow]
  }

  private def toSparkSqlType(value: Any): AnyRef = {
    value match {
      case date: Date => new Timestamp(date.getTime)
      case localDate: LocalDate => new java.sql.Date(subtractTimeZoneOffset(localDate.getMillisSinceEpoch))
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
  }
}

