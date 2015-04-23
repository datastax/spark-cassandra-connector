package com.datastax.spark.connector.cql


import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.types._
import org.scalatest.Inspectors._


class SchemaSpec extends SparkCassandraITWordSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  conn.withSessionDo { session =>
    session.execute(
      """CREATE KEYSPACE IF NOT EXISTS schema_test
        |  WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""".stripMargin)
    session.execute(
      """CREATE TYPE schema_test.address (street varchar, city varchar, zip int)""")
    session.execute(
      """CREATE TABLE schema_test.test(
        |  k1 int,
        |  k2 varchar,
        |  k3 timestamp,
        |  c1 bigint,
        |  c2 varchar,
        |  c3 uuid,
        |  d1_blob blob,
        |  d2_boolean boolean,
        |  d3_decimal decimal,
        |  d4_double double,
        |  d5_float float,
        |  d6_inet inet,
        |  d7_int int,
        |  d8_list list<int>,
        |  d9_map map<int, varchar>,
        |  d10_set set<int>,
        |  d11_timestamp timestamp,
        |  d12_uuid uuid,
        |  d13_timeuuid timeuuid,
        |  d14_varchar varchar,
        |  d15_varint varint,
        |  d16_address frozen<address>,
        |  PRIMARY KEY ((k1, k2, k3), c1, c2, c3)
        |)
      """.stripMargin)
  }

  val schema = Schema.fromCassandra(conn)

  "A Schema" should {
    "allow to get a list of keyspaces" in {
      schema.keyspaces.map(_.keyspaceName) should contain("schema_test")
    }
    "allow to look up a keyspace by name" in {
      val ks = schema.keyspaceByName("schema_test")
      ks.keyspaceName shouldBe "schema_test"
    }
  }

  "A KeyspaceDef" should {
    "allow to get a list of tables in the given keyspace" in {
      val ks = schema.keyspaceByName("schema_test")
      ks.tables.map(_.tableName).toSet shouldBe Set("test")
    }
    "allow to look up a table by name" in {
      val ks = schema.keyspaceByName("schema_test")
      val table = ks.tableByName("test")
      table.tableName shouldBe "test"
    }
  }

  "A TableDef" should {
    val ks = schema.keyspaceByName("schema_test")
    val table = ks.tableByName("test")

    "allow to read column definitions by name" in {
      table.columnByName("k1").columnName shouldBe "k1"
    }

    "allow to read primary key column definitions" in {
      table.primaryKey.size shouldBe 6
      table.primaryKey.map(_.columnName).toSeq shouldBe Seq(
        "k1", "k2", "k3", "c1", "c2", "c3")
      table.primaryKey.map(_.columnType).toSeq shouldBe Seq(
        IntType, VarCharType, TimestampType,  BigIntType, VarCharType, UUIDType)
      forAll(table.primaryKey) { c => c.isPrimaryKeyColumn shouldBe true }
    }

    "allow to read partitioning key column definitions" in {
      table.partitionKey.size shouldBe 3
      table.partitionKey.map(_.columnName).toSeq shouldBe Seq("k1", "k2", "k3")
      forAll(table.partitionKey) { c => c.isPartitionKeyColumn shouldBe true }
      forAll(table.partitionKey) { c => c.isPrimaryKeyColumn shouldBe true }
    }

    "allow to read regular column definitions" in {
      val columns = table.regularColumns
      columns.size shouldBe 16
      columns.map(_.columnName).toSet shouldBe Set(
        "d1_blob", "d2_boolean", "d3_decimal", "d4_double", "d5_float",
        "d6_inet", "d7_int", "d8_list", "d9_map", "d10_set",
        "d11_timestamp", "d12_uuid", "d13_timeuuid", "d14_varchar",
        "d15_varint", "d16_address")
    }

    "allow to read proper types of columns" in {
      table.columnByName("d1_blob").columnType shouldBe BlobType
      table.columnByName("d2_boolean").columnType shouldBe BooleanType
      table.columnByName("d3_decimal").columnType shouldBe DecimalType
      table.columnByName("d4_double").columnType shouldBe DoubleType
      table.columnByName("d5_float").columnType shouldBe FloatType
      table.columnByName("d6_inet").columnType shouldBe InetType
      table.columnByName("d7_int").columnType shouldBe IntType
      table.columnByName("d8_list").columnType shouldBe ListType(IntType)
      table.columnByName("d9_map").columnType shouldBe MapType(IntType, VarCharType)
      table.columnByName("d10_set").columnType shouldBe SetType(IntType)
      table.columnByName("d11_timestamp").columnType shouldBe TimestampType
      table.columnByName("d12_uuid").columnType shouldBe UUIDType
      table.columnByName("d13_timeuuid").columnType shouldBe TimeUUIDType
      table.columnByName("d14_varchar").columnType shouldBe VarCharType
      table.columnByName("d15_varint").columnType shouldBe VarIntType
      table.columnByName("d16_address").columnType shouldBe a [UserDefinedType]
    }

    "allow to list fields of a user defined type" in {
      val udt = table.columnByName("d16_address").columnType.asInstanceOf[UserDefinedType]
      udt.columnNames shouldBe Seq("street", "city", "zip")
      udt.columnTypes shouldBe Seq(VarCharType, VarCharType, IntType)
    }
  }

}
