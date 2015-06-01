package com.datastax.spark.connector.japi

import com.datastax.driver.core.{ProtocolVersion, Row}
import com.datastax.spark.connector.GettableData

final class CassandraRow(val columnNames: IndexedSeq[String], val columnValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable {

  private[spark] def this() = this(null: IndexedSeq[String], null) // required by Kryo for deserialization :(

  def this(columnNames: Array[String], columnValues: Array[AnyRef]) =
    this(columnNames.toIndexedSeq, columnValues.toIndexedSeq)

  protected def fieldNames = columnNames
  protected def fieldValues = columnValues

  def iterator = columnValues.iterator
  override def toString = "CassandraRow" + dataAsString
}


object CassandraRow {

  /** Deserializes first n columns from the given `Row` and returns them as
    * a `CassandraRow` object. The number of columns retrieved is determined by the length
    * of the columnNames argument. The columnNames argument is used as metadata for
    * the newly created `CassandraRow`, but it is not used to fetch data from
    * the input `Row` in order to improve performance. Fetching column values by name is much
    * slower than fetching by index. */
  def fromJavaDriverRow(row: Row, columnNames: Array[String])(implicit protocolVersion: ProtocolVersion): CassandraRow = {
    val data = new Array[Object](columnNames.length)
    for (i <- columnNames.indices)
      data(i) = GettableData.get(row, i)
    new CassandraRow(columnNames, data)
  }

  /** Creates a CassandraRow object from a map with keys denoting column names and
    * values denoting column values. */
  def fromMap(map: Map[String, Any]): CassandraRow = {
    val (columnNames, values) = map.unzip
    new CassandraRow(columnNames.toArray, values.map(_.asInstanceOf[AnyRef]).toArray)
  }

}
