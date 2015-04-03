package org.apache.spark.sql.cassandra

import java.sql.Timestamp
import java.util.Date

import com.datastax.driver.core.{Row, ProtocolVersion}
import com.datastax.spark.connector.AbstractGettableData
import com.datastax.spark.connector.rdd.reader.{ThisRowReaderAsFactory, RowReader}
import com.datastax.spark.connector.types.TypeConverter
import org.apache.spark.sql.catalyst.expressions.{Row => SparkRow}

final class CassandraSQLRow(columnNames: IndexedSeq[String], val fieldValues: IndexedSeq[AnyRef])
  extends AbstractGettableData with SparkRow with Serializable {

  protected def fieldNames = columnNames

  private[spark] def this() = this(null, null) // required by Kryo for deserialization :(

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  private def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(fieldValues(index))

  override def apply(i: Int) = fieldValues(i)
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
  override def toSeq: Seq[Any] = fieldValues
}


object CassandraSQLRow {

  def fromJavaDriverRow(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): CassandraSQLRow = {
    val data = new Array[Object](columnNames.length)
    for (i <- 0 until columnNames.length) {
      data(i) = AbstractGettableData.get(row, i)
      if (data(i).isInstanceOf[Date])
        data(i) = new Timestamp(data(i).asInstanceOf[Date].getTime)
    }
    new CassandraSQLRow(columnNames, data)
  }

  implicit object CassandraSQLRowReader extends RowReader[CassandraSQLRow] with ThisRowReaderAsFactory[CassandraSQLRow] {

    override def read(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): CassandraSQLRow =
      fromJavaDriverRow(row, columnNames)

    override def requiredColumns = None
    override def columnNames = None
    override def targetClass = classOf[CassandraSQLRow]
  }
}