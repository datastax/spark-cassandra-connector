package com.datastax.spark.connector

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.types.TypeConverter.StringConverter
import org.joda.time.DateTime

trait ScalaGettableData extends GettableData {

  /** Converts this row to a Map */
  def toMap: Map[String, Any] =
    columnNames.zip(columnValues).toMap

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(columnValues(index)) match {
      case null => throw new NullPointerException(
        "Unexpected null value of column " + index + ". Use get[Option[...]] to receive null values.")
      case notNull => notNull
    }

  /** Generic getter for getting columns of any type.
    * Looks the column up by column name. Column names are case-sensitive.*/
  def get[T](name: String)(implicit c: TypeConverter[T]): T =
    get[T](_indexOfOrThrow(name))

  /** Returns a `bool` column value. Besides working with `bool` Cassandra type, it can also read
    * numbers and strings. Non-zero numbers are converted to `true`, zero is converted to `false`.
    * Strings are converted using `String#toBoolean` method.*/
  def getBoolean(index: Int) = get[Boolean](index)
  def getBoolean(name: String) = get[Boolean](name)

  def getBooleanOption(index: Int) = get[Option[Boolean]](index)
  def getBooleanOption(name: String) = get[Option[Boolean]](name)

  def getByte(index: Int) = get[Byte](index)
  def getByte(name: String) = get[Byte](name)

  def getByteOption(index: Int) = get[Option[Byte]](index)
  def getByteOption(name: String) = get[Option[Byte]](name)

  def getShort(index: Int) = get[Short](index)
  def getShort(name: String) = get[Short](name)

  def getShortOption(index: Int) = get[Option[Short]](index)
  def getShortOption(name: String) = get[Option[Short]](name)

  /** Returns a column value as a 32-bit integer number.
    * Besides working with `int` Cassandra type, it can also read
    * other integer numbers as `bigint` or `varint` and strings.
    * The string must represent a valid integer number.
    * The number must be within 32-bit integer range or the `TypeConversionException` will be thrown.*/
  def getInt(index: Int) = get[Int](index)
  def getInt(name: String) = get[Int](name)

  def getIntOption(index: Int) = get[Option[Int]](index)
  def getIntOption(name: String) = get[Option[Int]](name)

  /** Returns a column value as a 64-bit integer number.
    * Recommended to use with `bigint` and `counter` CQL types
    * It can also read other column types as `int`, `varint`, `timestamp` and `string`.
    * The string must represent a valid integer number.
    * The number must be within 64-bit integer range or [[com.datastax.spark.connector.types.TypeConversionException]]
    * will be thrown. When used with timestamps, returns a number of milliseconds since epoch.*/
  def getLong(index: Int) = get[Long](index)
  def getLong(name: String) = get[Long](name)

  def getLongOption(index: Int) = get[Option[Long]](index)
  def getLongOption(name: String) = get[Option[Long]](name)

  /** Returns a column value as Float.
    * Recommended to use with `float` CQL type.
    * This method can be also used to read a `double` or `decimal` column, with some loss of precision.*/
  def getFloat(index: Int) = get[Float](index)
  def getFloat(name: String) = get[Float](name)

  def getFloatOption(index: Int) = get[Option[Float]](index)
  def getFloatOption(name: String) = get[Option[Float]](name)

  /** Returns a column value as Double.
    * Recommended to use with `float` and `double` CQL types.
    * This method can be also used to read a `decimal` column, with some loss of precision.*/
  def getDouble(index: Int) = get[Double](index)
  def getDouble(name: String) = get[Double](name)

  def getDoubleOption(index: Int) = get[Option[Double]](index)
  def getDoubleOption(name: String) = get[Option[Double]](name)

  /** Returns the column value converted to a `String` acceptable by CQL.
    * All data types that have human readable text representations can be converted.
    * Note, this is not the same as calling `getAny(index).toString` which works differently e.g. for dates.*/
  def getString(index: Int) = get[String](index)
  def getString(name: String) = get[String](name)

  def getStringOption(index: Int) = get[Option[String]](index)
  def getStringOption(name: String) = get[Option[String]](name)

  /** Returns a `blob` column value as ByteBuffer.
    * This method is not suitable for reading other types of columns.
    * Columns of type `blob` can be also read as Array[Byte] with the generic `get` method. */
  def getBytes(index: Int) = get[ByteBuffer](index)
  def getBytes(name: String) = get[ByteBuffer](name)

  def getBytesOption(index: Int) = get[Option[ByteBuffer]](index)
  def getBytesOption(name: String) = get[Option[ByteBuffer]](name)

  /** Returns a `timestamp` or `timeuuid` column value as `java.util.Date`.
    * To convert a timestamp to one of other supported date types, use the generic `get` method,
    * for example:
    * {{{
    *   row.get[java.sql.Date](0)
    * }}}*/
  def getDate(index: Int) = get[Date](index)
  def getDate(name: String) = get[Date](name)

  def getDateOption(index: Int) = get[Option[Date]](index)
  def getDateOption(name: String) = get[Option[Date]](name)

  /** Returns a `timestamp` or `timeuuid` column value as `org.joda.time.DateTime`. */
  def getDateTime(index: Int) = get[DateTime](index)
  def getDateTime(name: String) = get[DateTime](name)

  def getDateTimeOption(index: Int) = get[Option[DateTime]](index)
  def getDateTimeOption(name: String) = get[Option[DateTime]](name)

  /** Returns a `varint` column value.
    * Can be used with all other integer types as well as
    * with strings containing a valid integer number of arbitrary size. */
  def getVarInt(index: Int) = get[BigInt](index)
  def getVarInt(name: String) = get[BigInt](name)

  def getVarIntOption(index: Int) = get[Option[BigInt]](index)
  def getVarIntOption(name: String) = get[Option[BigInt]](name)

  /** Returns a `decimal` column value.
    * Can be used with all other floating point types as well as
    * with strings containing a valid floating point number of arbitrary precision. */
  def getDecimal(index: Int) = get[BigDecimal](index)
  def getDecimal(name: String) = get[BigDecimal](name)

  def getDecimalOption(index: Int) = get[Option[BigDecimal]](index)
  def getDecimalOption(name: String) = get[Option[BigDecimal]](name)

  /** Returns an `uuid` column value.
    * Can be used to read a string containing a valid UUID.*/
  def getUUID(index: Int) = get[UUID](index)
  def getUUID(name: String) = get[UUID](name)

  def getUUIDOption(index: Int) = get[Option[UUID]](index)
  def getUUIDOption(name: String) = get[Option[UUID]](name)

  /** Returns an `inet` column value.
    * Can be used to read a string containing a valid
    * Internet address, given either as a host name or IP address.*/
  def getInet(index: Int) = get[InetAddress](index)
  def getInet(name: String) = get[InetAddress](name)

  def getInetOption(index: Int) = get[Option[InetAddress]](index)
  def getInetOption(name: String) = get[Option[InetAddress]](name)

  /** Returns a column value of User Defined Type */
  def getUDTValue(index: Int) = get[UDTValue](index)
  def getUDTValue(name: String) = get[UDTValue](name)

  def getUDTValueOption(index: Int) = get[Option[UDTValue]](index)
  def getUDTValueOption(name: String) = get[Option[UDTValue]](name)

  /** Reads a `list` column value and returns it as Scala `Vector`.
    * A null list is converted to an empty collection.
    * Items of the list are converted to the given type.
    * This method can be also used to read `set` and `map` column types.
    * For `map`, the list items are converted to key-value pairs.
    * @tparam T type of the list item, must be given explicitly. */
  def getList[T : TypeConverter](index: Int) =
    get[Vector[T]](index)
  def getList[T : TypeConverter](name: String) =
    get[Vector[T]](name)

  /** Reads a `set` column value.
    * A null set is converted to an empty collection.
    * Items of the set are converted to the given type.
    * This method can be also used to read `list` and `map` column types.
    * For `map`, the set items are converted to key-value pairs.
    * @tparam T type of the set item, must be given explicitly. */
  def getSet[T : TypeConverter](index: Int) =
    get[Set[T]](index)
  def getSet[T : TypeConverter](name: String) =
    get[Set[T]](name)

  /** Reads a `map` column value.
    * A null map is converted to an empty collection.
    * Keys and values of the map are converted to the given types.
    * @tparam K type of keys, must be given explicitly.
    * @tparam V type of values, must be given explicitly.*/
  def getMap[K : TypeConverter, V : TypeConverter](index: Int) =
    get[Map[K, V]](index)
  def getMap[K : TypeConverter, V : TypeConverter](name: String) =
    get[Map[K, V]](name)




  def copy() = this  // this class is immutable
  def iterator = columnValues.iterator

}
