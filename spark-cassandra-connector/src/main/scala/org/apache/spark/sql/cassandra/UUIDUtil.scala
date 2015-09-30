package org.apache.spark.sql.cassandra

import java.nio.ByteBuffer
import java.util.UUID

object UUIDUtil {
  def asUuid(bytes: Array[Byte]): UUID = {
    val bb: ByteBuffer = ByteBuffer.wrap(bytes)
    val firstLong: Long = bb.getLong()
    val secondLong: Long = bb.getLong()
    return new UUID(firstLong, secondLong)
  }

  def asBytes(uuid: UUID): Array[Byte] = {
    val bb: ByteBuffer = ByteBuffer.wrap(new Array[Byte](16))
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    bb.array()
  }
}
