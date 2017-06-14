package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.{PartitionKeyColumns, SparkCassandraITFlatSpecBase}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.Schema
import com.datastax.driver.core.Token
import com.datastax.spark.connector.embedded.YamlTransformations
import com.datastax.spark.connector.writer.RowWriterFactory

import scala.collection.JavaConversions._
import scala.concurrent.Future

class TokenGeneratorSpec extends SparkCassandraITFlatSpecBase {

  case class SimpleKey(key: Int)
  case class ComplexKey(key1: Int, key2: Int, key3: String)

  useCassandraConfig(Seq(YamlTransformations.Default))
  override val conn = CassandraConnector(defaultConf)

  val simpleKeys = Seq(1, -500, 2801, 4000000, -95500).map(SimpleKey(_))
  val complexKey = simpleKeys.map{ case SimpleKey(x) => ComplexKey(x, x, x.toString)}

  conn.withSessionDo { session =>
    createKeyspace(session)
    awaitAll(
      Future{session.execute(s"CREATE TABLE $ks.simple(key INT PRIMARY KEY)")},
      Future{session.execute(s"CREATE TABLE $ks.complex(key1 INT, key2 INT, key3 text, PRIMARY KEY ((key1, key2, key3)))")}
    )
    val simplePS = session.prepare(s"INSERT INTO $ks.simple (key) VALUES (?)")
    val complexPS = session.prepare(s"INSERT INTO $ks.complex (key1, key2, key3) VALUES (?, ?, ?)")
    val insertFutures =
      simpleKeys.map( simpleKey =>
        session.executeAsync(simplePS.bind(simpleKey.key: java.lang.Integer))) ++
      complexKey.map( complexKey =>
        session.executeAsync(complexPS.bind(
          complexKey.key1: java.lang.Integer,
          complexKey.key2: java.lang.Integer,
          complexKey.key3: java.lang.String)))
    insertFutures.map(_.get())
  }

  val simpleTokenMap: Map[SimpleKey, Token] = conn.withSessionDo { session =>
    val resultSet = session.execute(s"SELECT key, TOKEN(key) FROM $ks.simple").all()
    resultSet.map(row => SimpleKey(row.getInt("key")) -> row.getPartitionKeyToken).toMap
  }

  val complexTokenMap: Map[ComplexKey, Token] = conn.withSessionDo { session =>
    val resultSet = session
      .execute(s"SELECT key1, key2, key3, TOKEN(key1, key2, key3) FROM $ks.complex")
      .all()

    resultSet.map(row =>
      ComplexKey(row.getInt("key1"), row.getInt("key2"), row.getString("key3")) -> row.getPartitionKeyToken
    ).toMap
  }

  val simpleTableDef = Schema.fromCassandra(conn, Some(ks), Some("simple")).tables.head
  val complexTableDef = Schema.fromCassandra(conn, Some(ks), Some("complex")).tables.head

  val simpleRW = implicitly[RowWriterFactory[SimpleKey]]
    .rowWriter(simpleTableDef, PartitionKeyColumns.selectFrom(simpleTableDef))

  val complexRW = implicitly[RowWriterFactory[ComplexKey]]
    .rowWriter(complexTableDef, PartitionKeyColumns.selectFrom(complexTableDef))

  "TokenGenerators" should "be able to determine simple partition keys" in {

    val tokenGenerator = new TokenGenerator[SimpleKey](conn, simpleTableDef, simpleRW)
    for ((key, token) <- simpleTokenMap) {
      tokenGenerator.getTokenFor(key) should be (token)
    }
  }

  it should "be able to determine composite keys" in {
    val tokenGenerator = new TokenGenerator[ComplexKey](conn, complexTableDef, complexRW)
    for ((key, token) <- complexTokenMap) {
      tokenGenerator.getTokenFor(key) should be (token)
    }
  }

}
