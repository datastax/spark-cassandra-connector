package com.datastax.spark.connector.writer

import java.nio.ByteBuffer

import com.datastax.driver.core.BoundStatement
import com.datastax.spark.connector.cql.TableDef

/** This class computes the routing key of a bound statement. */
class RoutingKeyGenerator(table: TableDef, columnNames: Seq[String])
  extends ((BoundStatement) => ByteBuffer) {

  private val partitionKeyIdxs = {
    val missing = table.partitionKey
      .map(_.columnName)
      .filterNot(columnNames.contains)
    require(
      missing.isEmpty,
      s"Not all partition key columns of ${table.name} were selected: [${missing.mkString(", ")}]")
    table.partitionKey
      .map(pkColumn => columnNames.indexOf(pkColumn.columnName))
      .filter(_ >= 0)
  }

  @transient
  protected lazy val routingKey = new ThreadLocal[Array[ByteBuffer]] {
    override def initialValue() = Array.ofDim[ByteBuffer](partitionKeyIdxs.size)
  }

  // this method is copied from Java Driver
  private def composeRoutingKeys(buffers: Array[ByteBuffer]): ByteBuffer = {
    val totalLength = buffers.map(_.remaining() + 3).sum
    val out = ByteBuffer.allocate(totalLength)

    for (buffer <- buffers) {
      val bb = buffer.duplicate
      out.put(((bb.remaining >> 8) & 0xFF).toByte)
      out.put((bb.remaining & 0xFF).toByte)
      out.put(bb)
      out.put(0.toByte)
    }
    out.flip
    out
  }

  private def fillRoutingKey(stmt: BoundStatement): Array[ByteBuffer] = {
    val rk = routingKey.get
    for (i <- 0 until partitionKeyIdxs.size)
      rk(i) = stmt.getBytesUnsafe(partitionKeyIdxs(i))
    rk
  }

  def apply(stmt: BoundStatement): ByteBuffer = {
    val rk = fillRoutingKey(stmt)
    if (rk.length == 1) rk(0) else composeRoutingKeys(rk)
  }

}
