package com.datastax.spark.connector

import java.nio.ByteBuffer

import com.datastax.driver.core.{ProtocolVersion, Row, UDTValue => DriverUDTValue}
import com.datastax.spark.connector.types.TypeConverter.StringConverter

import scala.collection.JavaConversions._

import com.datastax.spark.connector.util.ByteBufferUtil

trait GettableData {

  def columnNames: IndexedSeq[String]

  /** Corresponding values of every column, in the same order as `columnNames` */
  def columnValues: IndexedSeq[AnyRef]

  @transient
  private[connector] lazy val _indexOf =
    columnNames.zipWithIndex.toMap.withDefaultValue(-1)

  @transient
  private[connector] lazy val _indexOfOrThrow = _indexOf.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }

  /** Returns a column value by index without applying any conversion.
    * The underlying type is the same as the type returned by the low-level Cassandra driver,
    * is implementation defined and may change in the future.
    * Cassandra nulls are returned as Scala nulls. */
  def getRaw(index: Int): AnyRef = columnValues(index)
  def getRaw(name: String): AnyRef = columnValues(_indexOfOrThrow(name))

  /** Total number of columns in this row. Includes columns with null values. */
  def length = columnValues.size

  /** Total number of columns in this row. Includes columns with null values. */
  def size = columnValues.size

  /** Returns true if column value is Cassandra null */
  def isNullAt(index: Int): Boolean =
    columnValues(index) == null

  /** Returns true if column value is Cassandra null */
  def isNullAt(name: String): Boolean = {
    columnValues(_indexOfOrThrow(name)) == null
  }

  /** Returns index of column with given name or -1 if column not found */
  def indexOf(name: String): Int =
    _indexOf(name)

  /** Returns the name of the i-th column. */
  def nameOf(index: Int): String =
    columnNames(index)

  /** Returns true if column with given name is defined and has an
    * entry in the underlying value array, i.e. was requested in the result set.
    * For columns having null value, returns true. */
  def contains(name: String): Boolean =
    _indexOf(name) != -1

  /** Displays the content in human readable form, including the names and values of the columns */
  def dataAsString = columnNames
    .zip(columnValues)
    .map(kv => kv._1 + ": " + StringConverter.convert(kv._2))
    .mkString("{", ", ", "}")

  override def toString = dataAsString

  override def equals(o: Any) = o match {
    case o: GettableData =>
      if (this.columnValues.length == o.length) {
        this.columnValues.zip(o.columnValues).forall { case (mine, yours) => mine == yours}
      } else
        false
    case _ => false
  }
}

object GettableData {

  /* ByteBuffers are not serializable, so we need to convert them to something that is serializable.
     Array[Byte] seems reasonable candidate. Additionally converts Java collections to Scala ones. */
  private[connector] def convert(obj: Any)(implicit protocolVersion: ProtocolVersion): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.toArray(bb)
      case list: java.util.List[_] => list.view.map(convert).toList
      case set: java.util.Set[_] => set.view.map(convert).toSet
      case map: java.util.Map[_, _] => map.view.map { case (k, v) => (convert(k), convert(v))}.toMap
      case udtValue: DriverUDTValue => UDTValue.fromJavaDriverUDTValue(udtValue)
      case other => other.asInstanceOf[AnyRef]

    }
  }

  /** Deserializes given field from the DataStax Java Driver `Row` into appropriate Java type.
    * If the field is null, returns null (not Scala Option). */
  def get(row: Row, index: Int)(implicit protocolVersion: ProtocolVersion): AnyRef = {
    val columnDefinitions = row.getColumnDefinitions
    val columnType = columnDefinitions.getType(index)
    val bytes = row.getBytesUnsafe(index)
    if (bytes != null)
      convert(columnType.deserialize(bytes, protocolVersion))
    else
      null
  }

  def get(row: Row, name: String)(implicit protocolVersion: ProtocolVersion): AnyRef = {
    val index = row.getColumnDefinitions.getIndexOf(name)
    get(row, index)
  }

  def get(value: DriverUDTValue, name: String)(implicit protocolVersion: ProtocolVersion): AnyRef = {
    val valueType = value.getType.getFieldType(name)
    val bytes = value.getBytesUnsafe(name)
    if (bytes != null)
      convert(valueType.deserialize(bytes, protocolVersion))
    else
      null
  }
}

/** Thrown when the requested column does not exist in the result set. */
class ColumnNotFoundException(message: String) extends Exception(message)
