package com.datastax.spark.connector.util

import java.nio.ByteBuffer

object ByteBufferUtil {

  /** Copies the remaining bytes of the buffer into the given array, starting from offset zero.
    * The array must have capacity to store all of the remaining bytes of the buffer.
    * The buffer's position remains untouched. */
  def copyBuffer(src: ByteBuffer, dest: Array[Byte]): Array[Byte] = {
    if (src.hasArray) {
      val length = src.remaining
      val offset =  src.arrayOffset + src.position
      System.arraycopy(src.array, offset, dest, 0, length)
    } else {
      src.duplicate.get(dest)
    }
    dest
  }

  /** Converts a byte buffer into an array.
    * The buffer's position remains untouched. */
  def toArray(buffer: ByteBuffer): Array[Byte] = {
    if (buffer.hasArray && 
          buffer.arrayOffset + buffer.position == 0 && 
          buffer.remaining == buffer.array.length) {
      buffer.array
    } else {
      val dest = new Array[Byte](buffer.remaining)
      copyBuffer(buffer, dest)
    }
  }
}
