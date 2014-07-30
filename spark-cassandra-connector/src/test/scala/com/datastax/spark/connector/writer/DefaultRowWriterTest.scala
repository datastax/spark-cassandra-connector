package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import org.apache.commons.lang3.SerializationUtils
import org.junit.Test

class DefaultRowWriterTest {

  @Test
  def testSerializability() {
    val table = TableDef("test", "table", Nil, Nil, Nil)
    val rowWriter = new DefaultRowWriter[DefaultRowWriterTest](table, Nil)
    SerializationUtils.roundtrip(rowWriter)
  }

}
