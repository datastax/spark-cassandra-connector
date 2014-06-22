package com.datastax.driver.spark.rdd

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.datastax.driver.core.Row
import com.datastax.driver.spark.types.TypeConverter
import org.apache.cassandra.utils.ByteBufferUtil


/** Serves the same purpose as {{{Row}}} but is serializable and returns {{{Option}}} rather than null.
  * Additionally, value getters try to convert type, whenever possible. Therefore you are allowed to call
  * {{{getLong}}} on an {{{Int}}} field or {{{getString}}} on everything. */
class CassandraRow(data: Array[AnyRef], columnNames: Array[String]) extends Serializable {

  @transient
  private lazy val indexOf = columnNames.zipWithIndex.toMap

  def size = data.size

  def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(data(index))

  def get[T](name: String)(implicit c: TypeConverter[T]): T =
    get[T](indexOf(name))

  def getAny(index: Int) = get[Any](index)
  def getAny(name: String) = get[Any](name)

  def getAnyOption(index: Int) = get[Option[Any]](index)
  def getAnyOption(name: String) = get[Option[Any]](name)

  def getAnyRef(index: Int) = get[AnyRef](index)
  def getAnyRef(name: String) = get[AnyRef](name)

  def getAnyRefOption(index: Int) = get[Option[AnyRef]](index)
  def getAnyRefOption(name: String) = get[Option[AnyRef]](name)

  def getBool(index: Int) = get[Boolean](index)
  def getBool(name: String) = get[Boolean](name)

  def getBoolOption(index: Int) = get[Option[Boolean]](index)
  def getBoolOption(name: String) = get[Option[Boolean]](name)

  def getInt(index: Int) = get[Int](index)
  def getInt(name: String) = get[Int](name)

  def getIntOption(index: Int) = get[Option[Int]](index)
  def getIntOption(name: String) = get[Option[Int]](name)

  def getLong(index: Int) = get[Long](index)
  def getLong(name: String) = get[Long](name)

  def getLongOption(index: Int) = get[Option[Long]](index)
  def getLongOption(name: String) = get[Option[Long]](name)

  def getFloat(index: Int) = get[Float](index)
  def getFloat(name: String) = get[Float](name)

  def getFloatOption(index: Int) = get[Option[Float]](index)
  def getFloatOption(name: String) = get[Option[Float]](name)

  def getDouble(index: Int) = get[Double](index)
  def getDouble(name: String) = get[Double](name)

  def getDoubleOption(index: Int) = get[Option[Double]](index)
  def getDoubleOption(name: String) = get[Option[Double]](name)

  def getString(index: Int) = get[String](index)
  def getString(name: String) = get[String](name)

  def getStringOption(index: Int) = get[Option[String]](index)
  def getStringOption(name: String) = get[Option[String]](name)

  def getBytes(index: Int) = get[ByteBuffer](index)
  def getBytes(name: String) = get[ByteBuffer](name)

  def getBytesOption(index: Int) = get[Option[ByteBuffer]](index)
  def getBytesOption(name: String) = get[Option[ByteBuffer]](name)

  def getDate(index: Int) = get[Date](index)
  def getDate(name: String) = get[Date](name)

  def getDateOption(index: Int) = get[Option[Date]](index)
  def getDateOption(name: String) = get[Option[Date]](name)

  def getVarInt(index: Int) = get[BigInt](index)
  def getVarInt(name: String) = get[BigInt](name)

  def getVarIntOption(index: Int) = get[Option[BigInt]](index)
  def getVarIntOption(name: String) = get[Option[BigInt]](name)

  def getDecimal(index: Int) = get[BigDecimal](index)
  def getDecimal(name: String) = get[BigDecimal](name)

  def getDecimalOption(index: Int) = get[Option[BigDecimal]](index)
  def getDecimalOption(name: String) = get[Option[BigDecimal]](name)

  def getUUID(index: Int) = get[UUID](index)
  def getUUID(name: String) = get[UUID](name)

  def getUUIDOption(index: Int) = get[Option[UUID]](index)
  def getUUIDOption(name: String) = get[Option[UUID]](name)

  def getInet(index: Int) = get[InetAddress](index)
  def getInet(name: String) = get[InetAddress](name)

  def getInetOption(index: Int) = get[Option[InetAddress]](index)
  def getInetOption(name: String) = get[Option[InetAddress]](name)

  def getList[T : TypeConverter](index: Int) =
    get[Vector[T]](index)
  def getList[T : TypeConverter](name: String) =
    get[Vector[T]](name)

  def getSet[T : TypeConverter](index: Int) =
    get[Set[T]](index)
  def getSet[T : TypeConverter](name: String) =
    get[Set[T]](name)

  def getMap[K : TypeConverter, V : TypeConverter](index: Int) =
    get[Map[K, V]](index)
  def getMap[K : TypeConverter, V : TypeConverter](name: String) =
    get[Map[K, V]](name)

  override def toString = "CassandraRow" +
    columnNames.zip(data).map({ case (c, v) => c + ": " + v }).mkString("[", ", ", "]")
}


object CassandraRow {

  /** Deserializes given field from DataStax Java Driver {{{Row}}} into appropriate Java type.
   *  If the field is null, returns null (not Scala Option). */
  def get(row: Row, index: Int): AnyRef = {
    val columnDefinitions = row.getColumnDefinitions
    val columnType = columnDefinitions.getType(index)
    val columnValue = row.getBytesUnsafe(index)
    if (columnValue != null) {
      columnType.deserialize(columnValue) match {
        // ByteBuffers are not serializable, so we need to convert them to serializable arrays
        case buffer: ByteBuffer => ByteBufferUtil.getArray(buffer)
        case other => other
      }
    }
    else
      null
  }

  def get(row: Row, name: String): AnyRef = {
    val index = row.getColumnDefinitions.getIndexOf(name)
    get(row, index)
  }

  /** Deserializes first n columns from the given {{{Row}}} and returns them as
    * a {{{CassandraRow}}} object. The number of columns retrieved is determined by the length
    * of the columnNames argument. The columnNames argument is used as metadata for
    * the newly created {{{CassandraRow}}}, but it is not used to fetch data from
    * the input {{{Row}}} in order to improve performance. Fetching column values by name is much
    * slower than fetching by index. */
  def fromJavaDriverRow(row: Row, columnNames: Array[String]): CassandraRow = {
    val data = new Array[Object](columnNames.length)
    for (i <- 0 until columnNames.length)
        data(i) = get(row, i)
    new CassandraRow(data, columnNames)
  }

}
