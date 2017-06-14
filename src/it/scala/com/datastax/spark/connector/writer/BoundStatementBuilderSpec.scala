package com.datastax.spark.connector.writer

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.driver.core.ProtocolVersion
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.types.{CassandraOption, Unset}

class BoundStatementBuilderSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq(YamlTransformations.Default))
  override val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session, ks)
    session.execute( s"""CREATE TABLE IF NOT EXISTS "$ks".tab (id INT PRIMARY KEY, value TEXT)""")
  }
  val cluster = conn.withClusterDo(c => c)

  val schema = Schema.fromCassandra(conn, Some(ks), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, CassandraOption[String])]
    .rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "value"))
  val ps = conn.withSessionDo(session =>
    session.prepare( s"""INSERT INTO "$ks".tab (id, value) VALUES (?, ?) """))

  "BoundStatementBuilder" should "ignore Unset values if ProtocolVersion >= 4" in {
    val testProtocols = (ProtocolVersion.V4.toInt to ProtocolVersion.NEWEST_SUPPORTED.toInt)
      .map(ProtocolVersion.fromInt(_))

    for (testProtocol <- testProtocols) {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = testProtocol)
      val x = bsb.bind((1, CassandraOption.Unset))
      withClue(s"$testProtocol should ignore unset values :")(x.isSet("value") should be(false))
    }
  }

  it should "set Unset values to null if ProtocolVersion <= 3" in {
    for (testProtocol <- Seq(ProtocolVersion.V1, ProtocolVersion.V2, ProtocolVersion.V3)) {
      val bsb = new BoundStatementBuilder(rowWriter, ps, protocolVersion = testProtocol)
      val x = bsb.bind((1, CassandraOption.Unset))
      withClue(s"$testProtocol should set to null :")(x.isNull("value") should be(true))
    }
  }

  it should "ignore null values if ignoreNulls is set and protocol version >= 4" in {
    val testProtocols = (ProtocolVersion.V4.toInt to ProtocolVersion.NEWEST_SUPPORTED.toInt)
      .map(ProtocolVersion.fromInt(_))

    for (testProtocol <- testProtocols) {
      val bsb = new BoundStatementBuilder(
        rowWriter,
        ps,
        protocolVersion = testProtocol,
        ignoreNulls = true)

      val x = bsb.bind((1, null))
      withClue(s"$testProtocol should ignore unset values :")(x.isSet("value") should be(false))
    }
  }

  it should "throw an exception if ignoreNulls is set and protocol version <= 3" in {
    for (testProtocol <- Seq(ProtocolVersion.V1, ProtocolVersion.V2, ProtocolVersion.V3)) {
      intercept[IllegalArgumentException] {
        val bsb = new BoundStatementBuilder(
          rowWriter,
          ps,
          protocolVersion = testProtocol,
          ignoreNulls = true)
      }
    }
  }
}
