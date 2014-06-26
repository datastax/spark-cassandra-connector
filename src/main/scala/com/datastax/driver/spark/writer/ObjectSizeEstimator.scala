package com.datastax.driver.spark.writer

import java.io.{OutputStream, ObjectOutputStream}

/** Estimates amount of memory required to serialize Java/Scala objects */
object ObjectSizeEstimator {

  /** Records only how many bytes were written but the actual data is discarded */
  private class CountingOutputStream extends OutputStream {
    private var _length = 0
    override def write(b: Int) = _length += 1
    override def write(b: Array[Byte]) = _length += b.length
    override def write(b: Array[Byte], off: Int, len: Int) = _length += len
    def length = _length
  }

  /** Serializes passed objects and reports their total size */
  def measureSerializedSize(objects: Seq[Any]): Int = {
    val countingStream = new CountingOutputStream
    val objectStream = new ObjectOutputStream(countingStream)
    for (obj <- objects)
      objectStream.writeObject(obj.asInstanceOf[AnyRef])
    objectStream.close()
    countingStream.length
  }

}
