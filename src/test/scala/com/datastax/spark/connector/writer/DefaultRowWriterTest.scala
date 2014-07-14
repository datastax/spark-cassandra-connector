package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.util.SerializationUtil
import org.junit.Test

class DefaultRowWriterTest {

  @Test
  def testSerializability() {
    val table = TableDef("test", "table", Nil, Nil, Nil)
    val rowWriter = new DefaultRowWriter[DefaultRowWriterTest](table, Nil)
    SerializationUtil.serializeAndDeserialize(rowWriter)
  }

}
