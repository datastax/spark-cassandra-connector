package com.datastax.spark.connector.writer

import com.datastax.driver.core._
import com.datastax.spark.connector.types.{ColumnType, Unset}
import com.datastax.spark.connector.util.{CodecRegistryUtil, Logging}

/**
 * Class for binding row-like objects into prepared statements. prefixVals
 * is used for binding constant values into each bound statement. This supports parametrized
 * .where clauses in [[com.datastax.spark.connector.rdd.CassandraJoinRDD]]
 */
private[connector] class BoundStatementBuilder[T](
    val rowWriter: RowWriter[T],
    val preparedStmt: PreparedStatement,
    val prefixVals: Seq[Any] = Seq.empty,
    val ignoreNulls: Boolean = false,
    val protocolVersion: ProtocolVersion) extends Logging {

  private val columnNames = rowWriter.columnNames.toIndexedSeq
  private val columnTypes = columnNames.map(preparedStmt.getVariables.getType)
  private val converters = columnTypes.map(ColumnType.converterToCassandra(_))
  private val buffer = Array.ofDim[Any](columnNames.size)

  require(ignoreNulls == false || protocolVersion.toInt >= ProtocolVersion.V4.toInt,
    s"""
       |Protocol Version $protocolVersion does not support ignoring null values and leaving
       |parameters unset. This is only supported in ${ProtocolVersion.V4} and greater.
    """.stripMargin)

  var logUnsetToNullWarning = false
  val UnsetToNullWarning =
    s"""Unset values can only be used with C* >= 2.2. They have been replaced
        |with nulls. Found protocol version ${protocolVersion}.
        |${ProtocolVersion.V4} or greater required"
    """.stripMargin


  private def maybeLeaveUnset(
    boundStatement: BoundStatement,
    columnName: String): Unit = protocolVersion match {
      case pv if pv.toInt <= ProtocolVersion.V3.toInt => {
        boundStatement.setToNull(columnName)
        logUnsetToNullWarning = true
      }
      case _ =>
  }

  private def bindColumnNull(
    boundStatement: BoundStatement,
    columnName: String,
    columnType: DataType,
    columnValue: AnyRef): Unit = {

    if (columnValue == Unset || (ignoreNulls && columnValue == null)) {
      boundStatement.setToNull(columnName)
      logUnsetToNullWarning = true
    } else {
      val codec = CodecRegistryUtil.codecFor(columnType, columnValue)
      boundStatement.set(columnName, columnValue, codec)
    }
  }

  private def bindColumnUnset(
    boundStatement: BoundStatement,
    columnName: String,
    columnType: DataType,
    columnValue: AnyRef): Unit = {

    if (columnValue == Unset || (ignoreNulls && columnValue == null)) {
      //Do not bind
    } else {
      val codec = CodecRegistryUtil.codecFor(columnType, columnValue)
      boundStatement.set(columnName, columnValue, codec)
    }
  }

  /**
  * If the protocol version is greater than V3 (C* 2.2 and Greater) then
  * we can leave values in the prepared statement unset. If the version is
  * less than V3 then we need to place a `null` in the bound statement.
  */
  val bindColumn: (BoundStatement, String, DataType, AnyRef) => Unit = protocolVersion match {
    case pv if pv.toInt <= ProtocolVersion.V3.toInt => bindColumnNull
    case _ => bindColumnUnset
  }

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
      val columnType = columnTypes(i)
      val columnValue = converter.convert(buffer(i))
      bindColumn(boundStatement, columnName, columnType, columnValue)
      val serializedValue = boundStatement.getBytesUnsafe(i)
      if (serializedValue != null) bytesCount += serializedValue.remaining()
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
