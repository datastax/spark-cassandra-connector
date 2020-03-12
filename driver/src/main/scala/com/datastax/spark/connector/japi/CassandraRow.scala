package com.datastax.spark.connector.japi

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.CassandraRowMetadata

final class CassandraRow(val metaData:CassandraRowMetadata, val columnValues: IndexedSeq[AnyRef])
  extends JavaGettableData with Serializable {

  private[spark] def this() = this(null: CassandraRowMetadata, null: IndexedSeq[AnyRef]) // required by Kryo for deserialization :(

  def this(metaData: CassandraRowMetadata, columnValues: Array[AnyRef]) =
    this(metaData, columnValues.toIndexedSeq)

  /**
    * the consturctor is for testing and backward compatibility only.
    * Use default constructor with shared metadata for memory saving and performance.
    */
  def this(columnNames: Array[String], columnValues: Array[AnyRef]) =
    this(CassandraRowMetadata.fromColumnNames(columnNames.toIndexedSeq), columnValues.toIndexedSeq)

  protected def fieldNames = metaData
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
  def fromJavaDriverRow(row: Row, metaData: CassandraRowMetadata): CassandraRow = {
    new CassandraRow(metaData, com.datastax.spark.connector.CassandraRow.dataFromJavaDriverRow(row, metaData))
  }
  /** Creates a CassandraRow object from a map with keys denoting column names and
    * values denoting column values. */
  def fromMap(map: Map[String, Any]): CassandraRow = {
    val (columnNames, values) = map.unzip
    new CassandraRow(CassandraRowMetadata.fromColumnNames(columnNames.toIndexedSeq), values.map(_.asInstanceOf[AnyRef]).toArray)
  }

}
