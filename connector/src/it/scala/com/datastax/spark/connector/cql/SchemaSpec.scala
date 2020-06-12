package com.datastax.spark.connector.cql

import java.util.Optional

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.`type`.{DataTypes, ListType, MapType, SetType, UserDefinedType}
import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.util.{DriverUtil, schemaFromCassandra}
import org.scalatest.OptionValues._

import scala.collection.JavaConverters._

class SchemaSpec extends SparkCassandraITWordSpecBase with DefaultCluster {

  implicit def toCqlIdent(name:String): CqlIdentifier = CqlIdentifier.fromInternal(name)
  implicit def extractOption[T](joption:Optional[T]):T = DriverUtil.toOption(joption).value

  override lazy val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)

    session.execute(
      s"""CREATE TYPE $ks.address (street varchar, city varchar, zip int)""")
    session.execute(
      s"""CREATE TABLE $ks.test(
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
    session.execute(
      s"""CREATE INDEX test_d9_map_idx ON $ks.test (keys(d9_map))""")
    session.execute(
      s"""CREATE INDEX test_d7_int_idx ON $ks.test (d7_int)""")
  }

  val schema = schemaFromCassandra(conn)

  "A Schema" should {
    "allow to get a list of keyspaces" in {
      schema.keyspaces.map(_.getName) should contain(ks)
    }
    "allow to look up a keyspace by name" in {
      val keyspace = schema.keyspaceByName(ks)
      keyspace.getName shouldBe ks
    }
  }

  "A keyspace object backed by Java driver metadata" should {
    "allow to get a list of tables in the given keyspace" in {
      val keyspace = schema.keyspaceByName(ks)
      val tableMap = keyspace.getTables.asScala
      tableMap.keys shouldBe Set("test")
      tableMap.values.map(_.getName) shouldBe Set("test")
    }
    "allow to look up a table by name" in {
      val keyspace = schema.keyspaceByName(ks)
      val table = keyspace.getTable("test")
      table.getName shouldBe "test"
    }
    "allow to look up user type by name" in {
      val keyspace = schema.keyspaceByName(ks)
      val userType = keyspace.getUserDefinedType("address")
      userType.getName shouldBe "address"
    }
  }

  "A keyspace object backed by Java driver metadata" should {
    val keyspace = schema.keyspaceByName(ks)
    val table = keyspace.getTable("test")

    "allow to read column definitions by name" in {
      table.getColumn("k1").getName shouldBe "k1"
    }

    "allow to read primary key column definitions" in {
      val primaryKey = table.getPrimaryKey.asScala
      primaryKey.size shouldBe 6
      primaryKey.map(_.getName) shouldBe Seq(
        "k1", "k2", "k3", "c1", "c2", "c3")
      primaryKey.map(_.getType) shouldBe Seq(
        DataTypes.INT, DataTypes.TEXT, DataTypes.TIMESTAMP, DataTypes.BIGINT, DataTypes.TEXT, DataTypes.UUID)
    }

    "allow to read partitioning key column definitions" in {
      val partitionKey = table.getPartitionKey.asScala
      partitionKey.size shouldBe 3
      partitionKey.map(_.getName) shouldBe Seq("k1", "k2", "k3")
    }

    "allow to read regular column definitions" in {
      val columns = table.getColumns.asScala
      val expected = Set(
        "d1_blob", "d2_boolean", "d3_decimal", "d4_double", "d5_float",
        "d6_inet", "d7_int", "d8_list", "d9_map", "d10_set",
        "d11_timestamp", "d12_uuid", "d13_timeuuid", "d14_varchar",
        "d15_varint", "d16_address")
      columns.size shouldBe 16
      columns.keys.toSet shouldBe expected
      columns.values.map(_.getName).toSet shouldBe expected
    }

    "allow to read proper types of columns" in {
      table.getColumn("d1_blob").getType shouldBe DataTypes.BLOB
      table.getColumn("d1_blob").getType shouldBe DataTypes.BLOB
      table.getColumn("d2_boolean").getType shouldBe DataTypes.BOOLEAN
      table.getColumn("d3_decimal").getType shouldBe DataTypes.DECIMAL
      table.getColumn("d4_double").getType shouldBe DataTypes.DOUBLE
      table.getColumn("d5_float").getType shouldBe DataTypes.FLOAT
      table.getColumn("d6_inet").getType shouldBe DataTypes.INET
      table.getColumn("d7_int").getType shouldBe DataTypes.INT
      table.getColumn("d8_list").getType shouldBe a [ListType]
      table.getColumn("d9_map").getType shouldBe a [MapType]
      table.getColumn("d10_set").getType shouldBe a [SetType]
      table.getColumn("d11_timestamp").getType shouldBe DataTypes.TIMESTAMP
      table.getColumn("d12_uuid").getType shouldBe DataTypes.UUID
      table.getColumn("d13_timeuuid").getType shouldBe DataTypes.TIMEUUID
      table.getColumn("d14_varchar").getType shouldBe DataTypes.TEXT
      table.getColumn("d15_varint").getType shouldBe DataTypes.VARINT
      table.getColumn("d16_address").getType shouldBe a [UserDefinedType]
    }

    "allow to list fields of a user defined type" in {
      val udt = table.getColumn("d16_address").getType.asInstanceOf[UserDefinedType]
      udt.getFieldNames shouldBe Seq("street", "city", "zip")
      udt.getFieldTypes shouldBe Seq(DataTypes.TEXT, DataTypes.TEXT, DataTypes.INT)
    }

    "should not recognize column with collection index as indexed" in {
      val indexes = table.getIndexes.asScala
      val expected = "d7_int"
      indexes.size shouldBe 1
      indexes.keys.head shouldBe expected
      indexes.values.head.getName shouldBe expected
    }

    "should hold all indices retrieved from cassandra" in {
      table.getIndexes.asScala.size shouldBe 2
    }
  }

}
