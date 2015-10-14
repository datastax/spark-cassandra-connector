package org.apache.spark.sql.cassandra.udf

import java.util.UUID

import org.apache.spark.sql.cassandra.UUIDUtil


object UUIDUdf {

  def uuid(bytes: Array[Byte]): UUID = {
    UUIDUtil.asUuid(bytes)
  }

  def uuidToString(bytes: Array[Byte]): String = {
    uuid(bytes).toString
  }
}
