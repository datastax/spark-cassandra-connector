package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector._

class TableWriterColumnNamesSpec extends SparkCassandraITAbstractSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  case class KeyValue(key: Int, group: Long)

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
      val subset = Seq("key": NamedColumnRef, "group": NamedColumnRef)

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

    "distinguish and use only specified column names if provided, when aliases are specified" in {
      val subset = Seq[NamedColumnRef]("key" as "keyAlias", "group" as "groupAlias")

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

    "do not use TTL when it is not specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.defaultValue, timestamp = TimestampOption.defaultValue)
      )

      writer.queryTemplateUsingInsert should endWith (""")""")
    }

    "use static TTL if it is specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.constant(1234), timestamp = TimestampOption.defaultValue)
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL 1234""")
    }

    "use static timestamp if it is specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.defaultValue, timestamp = TimestampOption.constant(1400000000000L))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TIMESTAMP 1400000000000""")
    }

    "use both static TTL and static timestamp when they are specified" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.constant(1234), timestamp = TimestampOption.constant(1400000000000L))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL 1234 AND TIMESTAMP 1400000000000""")
    }

    "use per-row TTL and timestamp when the row writer provides them" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"), timestamp = TimestampOption.perRow("timestamp_column"))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL :ttl_column AND TIMESTAMP :timestamp_column""")
    }

    "use per-row TTL and static timestamp" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"), timestamp = TimestampOption.constant(1400000000000L))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL :ttl_column AND TIMESTAMP 1400000000000""")
    }

    "use per-row timestamp and static TTL" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.constant(1234), timestamp = TimestampOption.perRow("timestamp_column"))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL 1234 AND TIMESTAMP :timestamp_column""")
    }

    "use per-row TTL" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.perRow("ttl_column"), timestamp = TimestampOption.defaultValue)
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TTL :ttl_column""")
    }

    "use per-row timestamp" in {
      val writer = TableWriter(
        conn,
        keyspaceName = "column_names_test",
        tableName = "key_value",
        columnNames = AllColumns,
        writeConf = WriteConf(ttl = TTLOption.defaultValue, timestamp = TimestampOption.perRow("timestamp_column"))
      )

      writer.queryTemplateUsingInsert should endWith (""") USING TIMESTAMP :timestamp_column""")
    }
  }
}
