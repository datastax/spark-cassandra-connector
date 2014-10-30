package com.datastax.spark.connector

import java.nio.ByteBuffer

import com.datastax.driver.core.Row
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.JavaConversions._

private[connector] abstract class AbstractRow(val data: Array[AnyRef], val columnNames: Array[String]) extends Serializable {

  @transient
  private[connector] lazy val _indexOf =
    columnNames.zipWithIndex.toMap.withDefaultValue(-1)

  @transient
  private[connector] lazy val _indexOfOrThrow = _indexOf.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }

  /** Total number of columns in this row. Includes columns with null values. */
  def length = data.size

  /** Returns true if column value is Cassandra null */
  def isNullAt(index: Int): Boolean =
    data(index) == null

  /** Returns true if column value is Cassandra null */
  def isNullAt(name: String): Boolean = {
    data(_indexOfOrThrow(name)) == null
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

}

object AbstractRow {

  import com.datastax.spark.connector.cql.CassandraConnector.protocolVersion

  /* ByteBuffers are not serializable, so we need to convert them to something that is serializable.
     Array[Byte] seems reasonable candidate. Additionally converts Java collections to Scala ones. */
  private[connector] def convert(obj: Any): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.getArray(bb)
      case list: java.util.List[_] => list.view.map(convert).toList
      case set: java.util.Set[_] => set.view.map(convert).toSet
      case map: java.util.Map[_, _] => map.view.map { case (k, v) => (convert(k), convert(v))}.toMap
      case other => other.asInstanceOf[AnyRef]
    }
  }

  /** Deserializes given field from the DataStax Java Driver `Row` into appropriate Java type.
    * If the field is null, returns null (not Scala Option). */
  def get(row: Row, index: Int): AnyRef = {
    val columnDefinitions = row.getColumnDefinitions
    val columnType = columnDefinitions.getType(index)
    val columnValue = row.getBytesUnsafe(index)
    if (columnValue != null)
      convert(columnType.deserialize(columnValue, protocolVersion))
    else
      null
  }

  def get(row: Row, name: String): AnyRef = {
    val index = row.getColumnDefinitions.getIndexOf(name)
    get(row, index)
  }

}

/** Thrown when the requested column does not exist in the result set. */
class ColumnNotFoundException(message: String) extends Exception(message)
