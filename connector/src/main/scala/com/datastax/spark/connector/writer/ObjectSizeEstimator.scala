package com.datastax.spark.connector.writer

import java.io.{OutputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import scala.collection.JavaConversions._

import com.datastax.spark.connector.util.ByteBufferUtil


/** Estimates amount of memory required to serialize Java/Scala objects */
object ObjectSizeEstimator {

  private def makeSerializable(obj: Any): AnyRef = {
    obj match {
      case bb: ByteBuffer => ByteBufferUtil.toArray(bb)
      case list: java.util.List[_] => list.map(makeSerializable)
      case list: List[_] => list.map(makeSerializable)
      case set: java.util.Set[_] => set.map(makeSerializable)
      case set: Set[_] => set.map(makeSerializable)
      case map: java.util.Map[_, _] => map.map { case (k, v) => (makeSerializable(k), makeSerializable(v)) }
      case map: Map[_, _] => map.map { case (k, v) => (makeSerializable(k), makeSerializable(v)) }
      case other => other.asInstanceOf[AnyRef]
    }
  }

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
      objectStream.writeObject(makeSerializable(obj))
    objectStream.close()
    countingStream.length
  }

}
