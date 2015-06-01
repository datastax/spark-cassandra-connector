package com.datastax.spark.connector.japi


import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.math.{BigInteger, BigDecimal => JBigDecimal}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID, HashMap => JHashMap, List => JList, Map => JMap, Set => JSet}

import com.datastax.spark.connector.GettableData
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.types.TypeConverter.StringConverter
import org.joda.time.DateTime

trait JavaGettableData extends GettableData {

  /** Converts this row to a Map */
  def toMap: JMap[String, AnyRef] = {
    val map = new JHashMap[String, AnyRef]()
    for (i <- 0 until length) map.put(columnNames(i), columnValues(i))
    map
  }

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  def get[T <: AnyRef](index: Int, tc: TypeConverter[T]): T =
    _get(index)(tc)

  /** Generic getter for getting columns of any type.
    * Looks the column up by column name. Column names are case-sensitive.*/
  def get[T <: AnyRef](name: String, tc: TypeConverter[T]): T =
    _get(name)(tc)

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  private def _get[T <: AnyRef](index: Int)(implicit tc: TypeConverter[T]): T =
    tc.convert(columnValues(index))

  /** Generic getter for getting columns of any type.
    * Looks the column up by column name. Column names are case-sensitive.*/
  private def _get[T  <: AnyRef](name: String)(implicit tc: TypeConverter[T]): T =
    tc.convert(columnValues(_indexOfOrThrow(name)))

  /** Equivalent to `getAny` */
  def apply(index: Int): AnyRef = getObject(index)
  def apply(name: String): AnyRef = getObject(name)

  /** Returns a column value by index without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver. */
  def getObject(index: Int) = _get[Object](index)
  def getObject(name: String) = _get[Object](name)

  /** Returns a `bool` column value. Besides working with `bool` Cassandra type, it can also read
    * numbers and strings. Non-zero numbers are converted to `true`, zero is converted to `false`.
    * Strings are converted using `String#toBoolean` method.*/
  def getBoolean(index: Int) = _get[JBoolean](index)
  def getBoolean(name: String) = _get[JBoolean](name)

  def getByte(index: Int) = _get[JByte](index)
  def getByte(name: String) = _get[JByte](name)

  def getShort(index: Int) = _get[JShort](index)
  def getShort(name: String) = _get[JShort](name)

  /** Returns a column value as a 32-bit integer number.
    * Besides working with `int` Cassandra type, it can also read
    * other integer numbers as `bigint` or `varint` and strings.
    * The string must represent a valid integer number.
    * The number must be within 32-bit integer range or the `TypeConversionException` will be thrown.*/
  def getInt(index: Int) = _get[Integer](index)
  def getInt(name: String) = _get[Integer](name)

  /** Returns a column value as a 64-bit integer number.
    * Recommended to use with `bigint` and `counter` CQL types
    * It can also read other column types as `int`, `varint`, `timestamp` and `string`.
    * The string must represent a valid integer number.
    * The number must be within 64-bit integer range or
    * `com.datastax.spark.connector.types.TypeConversionException`
    * will be thrown. When used with timestamps, returns a number of milliseconds since epoch.*/
  def getLong(index: Int) = _get[JLong](index)
  def getLong(name: String) = _get[JLong](name)

  /** Returns a column value as Float.
    * Recommended to use with `float` CQL type.
    * This method can be also used to read a `double` or `decimal` column, with some loss of precision.*/
  def getFloat(index: Int) = _get[JFloat](index)
  def getFloat(name: String) = _get[JFloat](name)

  /** Returns a column value as Double.
    * Recommended to use with `float` and `double` CQL types.
    * This method can be also used to read a `decimal` column, with some loss of precision.*/
  def getDouble(index: Int) = _get[JDouble](index)
  def getDouble(name: String) = _get[JDouble](name)

  /** Returns the column value converted to a `String` acceptable by CQL.
    * All data types that have human readable text representations can be converted.
    * Note, this is not the same as calling `getAny(index).toString` which works differently e.g. for dates.*/
  def getString(index: Int) = _get[String](index)
  def getString(name: String) = _get[String](name)

  /** Returns a `blob` column value as ByteBuffer.
    * This method is not suitable for reading other types of columns.
    * Columns of type `blob` can be also read as Array[Byte] with the generic `get` method. */
  def getBytes(index: Int) = _get[ByteBuffer](index)
  def getBytes(name: String) = _get[ByteBuffer](name)

  /** Returns a `timestamp` or `timeuuid` column value as `java.util.Date`.
    * To convert a timestamp to one of other supported date types, use the generic `get` method,
    * for example:
    * {{{
    *   row.get[java.sql.Date](0)
    * }}}*/
  def getDate(index: Int) = _get[Date](index)
  def getDate(name: String) = _get[Date](name)

  /** Returns a `timestamp` or `timeuuid` column value as `org.joda.time.DateTime`. */
  def getDateTime(index: Int) = _get[DateTime](index)
  def getDateTime(name: String) = _get[DateTime](name)

  /** Returns a `varint` column value.
    * Can be used with all other integer types as well as
    * with strings containing a valid integer number of arbitrary size. */
  def getVarInt(index: Int) = _get[BigInteger](index)
  def getVarInt(name: String) = _get[BigInteger](name)

  /** Returns a `decimal` column value.
    * Can be used with all other floating point types as well as
    * with strings containing a valid floating point number of arbitrary precision. */
  def getDecimal(index: Int) = _get[JBigDecimal](index)
  def getDecimal(name: String) = _get[JBigDecimal](name)

  /** Returns an `uuid` column value.
    * Can be used to read a string containing a valid UUID.*/
  def getUUID(index: Int) = _get[UUID](index)
  def getUUID(name: String) = _get[UUID](name)

  /** Returns an `inet` column value.
    * Can be used to read a string containing a valid
    * Internet address, given either as a host name or IP address.*/
  def getInet(index: Int) = _get[InetAddress](index)
  def getInet(name: String) = _get[InetAddress](name)

  /** Returns a column value of User Defined Type */
  def getUDTValue(index: Int) = _get[UDTValue](index)
  def getUDTValue(name: String) = _get[UDTValue](name)

  /** Reads a `list` column value and returns it as Scala `Vector`.
    * A null list is converted to an empty collection.
    * Items of the list are converted to the given type.
    * This method can be also used to read `set` and `map` column types.
    * For `map`, the list items are converted to key-value pairs.*/
  def getList(index: Int) = _get[JList[AnyRef]](index)
  def getList(name: String) = _get[JList[AnyRef]](name)

  def getList[T](index: Int)(implicit converter: TypeConverter[T]) = _get[JList[T]](index)
  def getList[T](name: String)(implicit converter: TypeConverter[T]) = _get[JList[T]](name)

  /** Reads a `set` column value.
    * A null set is converted to an empty collection.
    * Items of the set are converted to the given type.
    * This method can be also used to read `list` and `map` column types.
    * For `map`, the set items are converted to key-value pairs. */
  def getSet(index: Int) = _get[JSet[AnyRef]](index)
  def getSet(name: String) = _get[JSet[AnyRef]](name)

  def getSet[T](index: Int)(implicit converter: TypeConverter[T]) = _get[JSet[T]](index)
  def getSet[T](name: String)(implicit converter: TypeConverter[T]) = _get[JSet[T]](name)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types. */
  def getMap(index: Int) = _get[JMap[AnyRef, AnyRef]](index)
  def getMap(name: String) = _get[JMap[AnyRef, AnyRef]](name)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types.
    * @tparam K type of keys, must be given explicitly.
    * @tparam V type of values, must be given explicitly.*/
  def getMap[K, V](index: Int)(implicit keyConverter: TypeConverter[K], valueConverter: TypeConverter[V]) =
    _get[JMap[K, V]](index)
  def getMap[K, V](name: String)(implicit keyConverter: TypeConverter[K], valueConverter: TypeConverter[V]) =
    _get[JMap[K, V]](name)

}
