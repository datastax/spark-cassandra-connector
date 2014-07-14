package com.datastax.spark.connector.util

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

object SerializationUtil {

  def serializeAndDeserialize[T <: AnyRef](obj: T): T = {
    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    os.writeObject(obj)
    os.close()
    val bis = new ByteArrayInputStream(bos.toByteArray)
    val is = new ObjectInputStream(bis)
    val result = is.readObject().asInstanceOf[T]
    is.close()
    result
  }

}
