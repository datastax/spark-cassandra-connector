package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.mapper.IndexedByNameColumnRef
import com.datastax.spark.connector.{SomeColumns, AllColumns}
import com.datastax.spark.connector.testkit._
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector._

class TableWriterColumnNamesSpec extends AbstractSpec with SharedEmbeddedCassandra with SparkTemplate {

  useCassandraConfig("cassandra-default.yaml.template")
  val conn = CassandraConnector(Set(cassandraHost))

  before {
    conn.withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS column_names_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS column_names_test.key_value (key INT, group BIGINT, value TEXT, PRIMARY KEY (key, group))")
      session.execute("TRUNCATE column_names_test.key_value")
    }
  }

  "TableWriter" must {
    "distinguish `AllColumns`" in {
      val all = Vector("key", "group", "value")

      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf()
      )

      writer.columnNames.size should be (all.size)
      writer.columnNames should be(all)
    }

    "distinguish and use only specified column names if provided" in {
      val subset = Seq("key": IndexedByNameColumnRef, "group": IndexedByNameColumnRef)

      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = SomeColumns(subset: _*),
        writeConf = WriteConf()
      )

      writer.columnNames.size should be (subset.size)
      writer.columnNames should be (Vector("key", "group"))
    }
    "fail in the RowWriter if provided specified column names do not include primary keys" in {
      import com.datastax.spark.connector._

      intercept[IllegalArgumentException] {
        sc.parallelize(Seq((1, 1L, None))).saveToCassandra("column_names_test", "key_value", SomeColumns("key", "value"))
      }
    }
  }
}
