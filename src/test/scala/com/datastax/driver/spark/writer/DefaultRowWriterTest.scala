package com.datastax.driver.spark.writer

import com.datastax.driver.spark.connector.TableDef
import com.datastax.driver.spark.util.SerializationUtil
import org.junit.Test

class DefaultRowWriterTest {

  @Test
  def testSerializability() {
    val table = TableDef("test", "table", Nil, Nil, Nil)
    val rowWriter = new DefaultRowWriter[DefaultRowWriterTest](table, Nil)
    SerializationUtil.serializeAndDeserialize(rowWriter)
  }

}
