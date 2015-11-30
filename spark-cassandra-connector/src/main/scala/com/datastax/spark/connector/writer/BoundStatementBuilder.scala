package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.types.ColumnType
import com.datastax.spark.connector.util.CodecRegistryUtil
import org.apache.spark.Logging

/**
 * Class for binding row-like objects into prepared statements. prefixVals
 * is used for binding constant values into each bound statement. This supports parametrized
 * .where clauses in [[com.datastax.spark.connector.rdd.CassandraJoinRDD]]
 */
private[connector] class BoundStatementBuilder[T](
    val rowWriter: RowWriter[T],
    val preparedStmt: PreparedStatement,
    val prefixVals: Seq[Any] = Seq.empty,
    val protocolVersion: ProtocolVersion) extends Logging {

  private val columnNames = rowWriter.columnNames.toIndexedSeq
  private val columnTypes = columnNames.map(preparedStmt.getVariables.getType)
  private val converters = columnTypes.map(ColumnType.converterToCassandra(_))
  private val buffer = Array.ofDim[Any](columnNames.size)

  private val prefixConverted = for {
    prefixIndex: Int <- 0 until prefixVals.length
    prefixVal = prefixVals(prefixIndex)
    prefixType = preparedStmt.getVariables.getType(prefixIndex)
    prefixConverter =  ColumnType.converterToCassandra(prefixType)
  } yield prefixConverter.convert(prefixVal)

  /** Creates `BoundStatement` from the given data item */
  def bind(row: T): RichBoundStatement = {
    val boundStatement = new RichBoundStatement(preparedStmt)
    boundStatement.bind(prefixConverted: _*)

    rowWriter.readColumnValues(row, buffer)
    var bytesCount = 0
    for (i <- 0 until columnNames.size) {
      val converter = converters(i)
      val columnName = columnNames(i)
      val columnValue = converter.convert(buffer(i))

      //C* 2.2 and Greater Allows us to leave fields Unset, we'll interpret Scala None as this unset value
      if (protocolVersion.toInt < ProtocolVersion.V4.toInt || columnValue != None) {
        if (columnValue == None){
          boundStatement.setToNull(columnName)
        } else {
          boundStatement.set(columnName, columnValue, CodecRegistryUtil.codecFor(columnTypes(i), columnValue))
        }
        val serializedValue = boundStatement.getBytesUnsafe(i)
        if (serializedValue != null) bytesCount += serializedValue.remaining()
      }

    }
    boundStatement.bytesCount = bytesCount
    boundStatement
  }

}

private[connector] object BoundStatementBuilder {
  /** Calculate bound statement size in bytes. */
  def calculateDataSize(stmt: BoundStatement): Int = {
    var size = 0
    for (i <- 0 until stmt.preparedStatement().getVariables.size())
      if (!stmt.isNull(i)) size += stmt.getBytesUnsafe(i).remaining()

    size
  }
}
