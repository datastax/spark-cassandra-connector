package com.datastax.spark.connector

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.datastax.driver.core.Row
import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.types.TypeConverter.StringConverter
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.JavaConversions._

/** Thrown when the requested column does not exist in the result set. */
class ColumnNotFoundException(message: String) extends Exception(message)

/** Represents a single row fetched from Cassandra.
  * Offers getters to read individual fields by column name or column index.
  * The getters try to convert value to desired type, whenever possible.
  * Most of the column types can be converted to a `String`.
  * For nullable columns, you should use the `getXXXOption` getters which convert
  * `null`s to `None` values, otherwise a `NullPointerException` would be thrown.
  *
  * All getters throw an exception if column name/index is not found.
  * Column indexes start at 0.
  *
  * If the value cannot be converted to desired type,
  * [[com.datastax.spark.connector.types.TypeConversionException]] is thrown.
  *
  * Recommended getters for Cassandra types:
  *
  *   - `ascii`:     `getString`, `getStringOption`
  *   - `bigint`:    `getLong`, `getLongOption`
  *   - `blob`:      `getBytes`, `getBytesOption`
  *   - `boolean`:   `getBool`, `getBoolOption`
  *   - `counter`:   `getLong`, `getLongOption`
  *   - `decimal`:   `getDecimal`, `getDecimalOption`
  *   - `double`:    `getDouble`, `getDoubleOption`
  *   - `float`:     `getFloat`, `getFloatOption`
  *   - `inet`:      `getInet`, `getInetOption`
  *   - `int`:       `getInt`, `getIntOption`
  *   - `text`:      `getString`, `getStringOption`
  *   - `timestamp`: `getDate`, `getDateOption`
  *   - `timeuuid`:  `getUUID`, `getUUIDOption`
  *   - `uuid`:      `getUUID`, `getUUIDOption`
  *   - `varchar`:   `getString`, `getStringOption`
  *   - `varint`:    `getVarInt`, `getVarIntOption`
  *   - `list`:      `getList[T]`
  *   - `set`:       `getSet[T]`
  *   - `map`:       `getMap[K, V]`
  *
  * Collection getters `getList`, `getSet` and `getMap` require to explicitly pass an appropriate item type:
  * {{{
  * row.getList[String]("a_list")
  * row.getList[Int]("a_list")
  * row.getMap[Int, String]("a_map")
  * }}}
  *
  * Generic `get` allows to automatically convert collections to other collection types.
  * Supported containers:
  *   - `scala.collection.immutable.List`
  *   - `scala.collection.immutable.Set`
  *   - `scala.collection.immutable.TreeSet`
  *   - `scala.collection.immutable.Vector`
  *   - `scala.collection.immutable.Map`
  *   - `scala.collection.immutable.TreeMap`
  *   - `scala.collection.Iterable`
  *   - `scala.collection.IndexedSeq`
  *   - `java.util.ArrayList`
  *   - `java.util.HashSet`
  *   - `java.util.HashMap`
  *
  * Example:
  * {{{
  * row.get[List[Int]]("a_list")
  * row.get[Vector[Int]]("a_list")
  * row.get[java.util.ArrayList[Int]]("a_list")
  * row.get[TreeMap[Int, String]]("a_map")
  * }}}
  *
  *
  * Timestamps can be converted to other Date types by using generic `get`. Supported date types:
  *   - java.util.Date
  *   - java.sql.Date
  *   - org.joda.time.DateTime
  */
class CassandraRow(data: Array[AnyRef], columnNames: Array[String]) extends Serializable {

  @transient
  private lazy val _indexOf =
    columnNames.zipWithIndex.toMap.withDefaultValue(-1)

  private lazy val _indexOfOrThrow = _indexOf.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }

  /** Total number of columns in this row. Includes columns with null values. */
  def size = data.size

  /** Returns true if column value is Cassandra null */
  def isNull(index: Int): Boolean =
    data(index) == null

  /** Returns true if column value is Cassandra null */
  def isNull(name: String): Boolean = {
    data(_indexOfOrThrow(name)) == null
  }

  /** Returns index of column with given name or -1 if column not found */
  def indexOf(name: String): Int =
    _indexOf(name)

  /** Returns true if column with given name is defined and has an
    * entry in the underlying value array, i.e. was requested in the result set.
    * For columns having null value, returns true.*/
  def contains(name: String): Boolean =
    _indexOf(name) != -1

  /** Generic getter for getting columns of any type.
    * Looks the column up by its index. First column starts at index 0. */
  def get[T](index: Int)(implicit c: TypeConverter[T]): T =
    c.convert(data(index))

  /** Generic getter for getting columns of any type.
    * Looks the column up by column name. Column names are case-sensitive.*/
  def get[T](name: String)(implicit c: TypeConverter[T]): T =
    get[T](_indexOfOrThrow(name))

  /** Returns a column value without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver.*/
  def getAny(index: Int) = get[Any](index)
  def getAny(name: String) = get[Any](name)

  /** Returns a column value without applying any conversion, besides converting a null to a None.
    * The underlying type is the same as the type returned by the low-level Cassandra driver.*/
  def getAnyOption(index: Int) = get[Option[Any]](index)
  def getAnyOption(name: String) = get[Option[Any]](name)

  /** Returns a column value by index without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver. */
  def getAnyRef(index: Int) = get[AnyRef](index)
  def getAnyRef(name: String) = get[AnyRef](name)

  /** Returns a column value without applying any conversion, besides converting a null to a None.
    * The underlying type is the same as the type returned by the low-level Cassandra driver. */
  def getAnyRefOption(index: Int) = get[Option[AnyRef]](index)
  def getAnyRefOption(name: String) = get[Option[AnyRef]](name)

  /** Returns a `bool` column value. Besides working with `bool` Cassandra type, it can also read
    * numbers and strings. Non-zero numbers are converted to `true`, zero is converted to `false`.
    * Strings are converted using `String#toBoolean` method.*/
  def getBool(index: Int) = get[Boolean](index)
  def getBool(name: String) = get[Boolean](name)

  def getBoolOption(index: Int) = get[Option[Boolean]](index)
  def getBoolOption(name: String) = get[Option[Boolean]](name)

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
    * To convert a timestamp to one of other supported date types, use the generic `get` method:
    * {{{
    *   row.get[java.sql.Date](0)
    *   row.get[org.joda.time.DateTime](0)
    * }}}*/
  def getDate(index: Int) = get[Date](index)
  def getDate(name: String) = get[Date](name)

  def getDateOption(index: Int) = get[Option[Date]](index)
  def getDateOption(name: String) = get[Option[Date]](name)

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

  /** Displays the row in human readable form, including the names and values of the columns */
  override def toString =
    "CassandraRow" + columnNames
      .zip(data)
      .map(kv => kv._1 + ": " + StringConverter.convert(kv._2))
      .mkString("{", ", ", "}")
}


object CassandraRow {

  /* ByteBuffers are not serializable, so we need to convert them to something that is serializable.
     Array[Byte] seems reasonable candidate. Additionally converts Java collections to Scala ones. */
  private def convert(obj: Any): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.getArray(bb)
      case list: java.util.List[_] => list.view.map(convert).toList
      case set: java.util.Set[_] => set.view.map(convert).toSet
      case map: java.util.Map[_, _] => map.view.map { case (k, v) => (convert(k), convert(v)) }.toMap
      case other => other.asInstanceOf[AnyRef]
    }
  }

  /** Deserializes given field from the DataStax Java Driver `Row` into appropriate Java type.
   *  If the field is null, returns null (not Scala Option). */
  def get(row: Row, index: Int): AnyRef = {
    val columnDefinitions = row.getColumnDefinitions
    val columnType = columnDefinitions.getType(index)
    val columnValue = row.getBytesUnsafe(index)
    if (columnValue != null)
      convert(columnType.deserialize(columnValue))
    else
      null
  }

  def get(row: Row, name: String): AnyRef = {
    val index = row.getColumnDefinitions.getIndexOf(name)
    get(row, index)
  }

  /** Deserializes first n columns from the given `Row` and returns them as
    * a `CassandraRow` object. The number of columns retrieved is determined by the length
    * of the columnNames argument. The columnNames argument is used as metadata for
    * the newly created `CassandraRow`, but it is not used to fetch data from
    * the input `Row` in order to improve performance. Fetching column values by name is much
    * slower than fetching by index. */
  def fromJavaDriverRow(row: Row, columnNames: Array[String]): CassandraRow = {
    val data = new Array[Object](columnNames.length)
    for (i <- 0 until columnNames.length)
        data(i) = get(row, i)
    new CassandraRow(data, columnNames)
  }

  /** Creates a CassandraRow object from a map with keys denoting column names and
    * values denoting column values. */
  def fromMap(map: Map[String, Any]): CassandraRow = {
    val (columnNames, values) = map.unzip
    new CassandraRow(values.map(_.asInstanceOf[AnyRef]).toArray, columnNames.toArray)
  }

}
