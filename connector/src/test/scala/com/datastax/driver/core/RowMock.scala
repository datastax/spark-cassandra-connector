package com.datastax.driver.core

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.cql.Row

class RowMock(columnSizes: Option[Int]*)
  extends AbstractGettableData(ProtocolVersion.DEFAULT) with Row {

  val bufs = columnSizes.map {
    case Some(size) => ByteBuffer.allocate(size)
    case _ => null
  }.toArray

  val defs = new ColumnDefinitions(
    columnSizes.map(i => new ColumnDefinitions.Definition("ks", "tab", s"c$i", DataType.text())).toArray,
    getCodecRegistry)

  override def getColumnDefinitions: ColumnDefinitions = defs

  override def getBytesUnsafe(i: Int): ByteBuffer = bufs(i)

  override def getBytesUnsafe(s: String): ByteBuffer = getBytesUnsafe(defs.firstIndexOf(s))

  override def isNull(i: Int): Boolean = bufs(i) == null

  override def isNull(s: String): Boolean = isNull(defs.firstIndexOf(s))

  override def getIndexOf(name: String): Int = ???

  override def getToken(i: Int): Token = ???

  override def getToken(name: String): Token = ???

  override def getPartitionKeyToken: Token = ???

  override def getType(i: Int): DataType = ???

  override def getValue(i: Int): ByteBuffer = ???

  override def getName(i: Int): String = ???

  override def getCodecRegistry = CodecRegistry.DEFAULT_INSTANCE
}
