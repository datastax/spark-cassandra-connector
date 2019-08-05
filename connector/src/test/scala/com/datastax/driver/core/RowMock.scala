package com.datastax.driver.core

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.cql.{ColumnDefinition, ColumnDefinitions, Row}
import com.datastax.oss.driver.api.core.detach.AttachmentPoint
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.oss.driver.api.core.{CqlIdentifier, ProtocolVersion}
import com.datastax.oss.driver.internal.core.cql.{DefaultColumnDefinition, DefaultColumnDefinitions}
import com.datastax.oss.protocol.internal.ProtocolConstants
import com.datastax.oss.protocol.internal.response.result.{ColumnSpec, RawType}

import scala.collection.JavaConverters._

class RowMock(columnSizes: Option[Int]*) extends Row {

  val bufs = columnSizes.map {
    case Some(size) => ByteBuffer.allocate(size)
    case _ => null
  }.toArray

  val defs = DefaultColumnDefinitions.valueOf(
    columnSizes.zipWithIndex.map { case (i, id) =>
      val columnSepc = new ColumnSpec("ks", "tab", s"c$i", id, RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))
      new DefaultColumnDefinition(columnSepc, null).asInstanceOf[ColumnDefinition]
    }.toList.asJava
  )

  override def getColumnDefinitions: ColumnDefinitions = defs

  override def getBytesUnsafe(i: Int): ByteBuffer = bufs(i)

  override def getBytesUnsafe(s: String): ByteBuffer = getBytesUnsafe(defs.firstIndexOf(s))

  override def isNull(i: Int): Boolean = bufs(i) == null

  override def isNull(s: String): Boolean = isNull(defs.firstIndexOf(s))

  override def getToken(i: Int): Token = ???

  override def getToken(name: String): Token = ???

  override def getType(i: Int): DataType = ???

  override def isDetached: Boolean = ???

  override def attach(attachmentPoint: AttachmentPoint): Unit = ???

  override def firstIndexOf(name: String): Int = ???

  override def getType(name: String): DataType = ???

  override def firstIndexOf(id: CqlIdentifier): Int = ???

  override def getType(id: CqlIdentifier): DataType = ???

  override def size(): Int = ???

  override def codecRegistry(): CodecRegistry = CodecRegistry.DEFAULT

  override def protocolVersion(): ProtocolVersion = ProtocolVersion.DEFAULT
}
