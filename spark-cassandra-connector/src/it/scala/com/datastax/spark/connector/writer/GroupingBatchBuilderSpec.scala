package com.datastax.spark.connector.writer

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.util.Random

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core.{BatchStatement, BoundStatement, ConsistencyLevel, Session}
import com.datastax.spark.connector.cql.{CassandraConnector, Schema}
import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.{BatchSize, BytesInBatch, RowsInBatch, SparkCassandraITFlatSpecBase}

class GroupingBatchBuilderSpec extends SparkCassandraITFlatSpecBase {

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  val conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))

  private val ks = "GroupingBatchBuilder"

  conn.withSessionDo { session =>
    session.execute(s"""CREATE KEYSPACE IF NOT EXISTS "$ks" WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }""")
    session.execute(s"""CREATE TABLE IF NOT EXISTS "$ks".tab (id INT PRIMARY KEY, value TEXT)""")
  }

  val protocolVersion = conn.withClusterDo(_.getConfiguration.getProtocolOptions.getProtocolVersionEnum)
  val schema = Schema.fromCassandra(conn, Some(ks), Some("tab"))
  val rowWriter = RowWriterFactory.defaultRowWriterFactory[(Int, String)].rowWriter(schema.tables.head, IndexedSeq("id", "value"))
  val rkg = new RoutingKeyGenerator(schema.tables.head, Seq("id", "value"))

  def makeBatchBuilder(session: Session): (BoundStatement => Any, BatchSize, Int, Iterator[(Int, String)]) => GroupingBatchBuilder[(Int, String)] = {
    val stmt = session.prepare(s"""INSERT INTO "$ks".tab (id, value) VALUES (:id, :value)""")
    val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion)
    val batchStmtBuilder = new BatchStatementBuilder(Type.UNLOGGED, rkg, ConsistencyLevel.LOCAL_ONE)
    new GroupingBatchBuilder[(Int, String)](boundStmtBuilder, batchStmtBuilder, _: BoundStatement => Any, _: BatchSize, _: Int, _: Iterator[(Int, String)])
  }

  def staticBatchKeyGen(bs: BoundStatement): Int = 0

  def dynamicBatchKeyGen(bs: BoundStatement): Int = bs.getInt(0) % 2

  def dynamicBatchKeyGen5(bs: BoundStatement): Int = bs.getInt(0) % 5

  "GroupingBatchBuilder in fixed batch key mode" should "make bound statements when batch size is specified as RowsInBatch(1)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(staticBatchKeyGen, RowsInBatch(1), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[BoundStatement])
      statements should have size 3
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements when batch size is specified as BytesInBatch(0)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(staticBatchKeyGen, BytesInBatch(0), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[BoundStatement])
      statements should have size 3
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make a batch and a bound statements according to the number of statements in a group" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(staticBatchKeyGen, RowsInBatch(2), 1, data.toIterator).toList
      statements should have size 2
      statements(0) shouldBe a[BatchStatement]
      statements(1) shouldBe a[BoundStatement]
      statements.flatMap {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make equal batches when batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      val statements = bm(staticBatchKeyGen, RowsInBatch(2), 1, data.toIterator).toList
      statements should have size 2
      statements foreach {
        _ shouldBe a[BatchStatement]
      }
      statements.flatMap {
        case s: BatchStatement =>
          s.size() should be(2)
          s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make batches of size not greater than the size specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq(
        (1, "a"), // 5 bytes
        (2, "aa"), // 6 bytes
        (3, "aaa"), // 7 bytes
        (4, "aaaa"), // 8 bytes
        (5, "aaaaa"), // 9 bytes
        (6, "aaaaaa"), // 10 bytes
        (7, "aaaaaaaaaaaa") // 16 bytes
      )
      val statements = bm(staticBatchKeyGen, BytesInBatch(15), 1, data.toIterator).toList
      statements should have size 5
      statements.take(2) foreach {
        _ shouldBe a[BatchStatement]
      }
      statements.drop(2).take(3) foreach {
        _ shouldBe a[BoundStatement]
      }

      val stmtss = statements.map {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.getStatements.map(_.asInstanceOf[BoundStatement]).toList
      }

      stmtss.foreach(stmts => stmts.size should be > 0)
      stmtss.foreach(stmts => if (stmts.size > 1) stmts.map(BoundStatementBuilder.calculateDataSize).sum should be <= 15)
      stmtss.flatten.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsInOrderAs data
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(staticBatchKeyGen, RowsInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(staticBatchKeyGen, BytesInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  "GroupingBatchBuilder in dynamic batch key mode" should "make bound statements when batch size is specified as RowsInBatch(1)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(1), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[BoundStatement])
      statements should have size 3
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements when batch size is specified as BytesInBatch(0)" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, BytesInBatch(0), 1, data.toIterator).toList
      statements.foreach(_ shouldBe a[BoundStatement])
      statements should have size 3
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make a batch and a bound statements according to the number of statements in a group and a batch key" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(2), 2, data.toIterator).toList
      statements should have size 2
      statements(0) shouldBe a[BatchStatement]
      statements(1) shouldBe a[BoundStatement]
      statements.flatMap {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make bound statements if batches cannot be made due to imposed limits" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(2), 1, data.toIterator).toList
      statements should have size 3
      statements.foreach(_ shouldBe a[BoundStatement])
      statements.map(s => s.asInstanceOf[BoundStatement]).map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make equal batches when batch size is specified in rows and batch buffer is enough" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq((1, "one"), (2, "two"), (3, "three"), (4, "four"))
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(2), 2, data.toIterator).toList
      statements should have size 2
      statements foreach {
        _ shouldBe a[BatchStatement]
      }
      statements.flatMap {
        case s: BatchStatement =>
          s.size() should be(2)
          s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "make batches of size not greater than the size specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq(
        (1, "a"), // 5 bytes
        (2, "aa"), // 6 bytes
        (3, "aaa"), // 7 bytes
        (4, "aaaa"), // 8 bytes
        (5, "aaaaa"), // 9 bytes
        (6, "aaaaaa"), // 10 bytes
        (7, "aaaaaaaaaaaa") // 16 bytes
      )
      // we expect this:
      // 1: (1, 3), (5), (7)
      // 0: (2, 4), (6)

      val statements = bm(dynamicBatchKeyGen, BytesInBatch(15), 2, data.toIterator).toList
      statements should have size 5

      val boundStatements = statements collect {
        case s: BoundStatement => s.getInt(0)
      }
      boundStatements should have size 3
      boundStatements should contain theSameElementsAs Seq(5, 6, 7)

      val batchStatements = statements collect {
        case s: BatchStatement if s.size() == 2 => s.getStatements.map(_.asInstanceOf[BoundStatement].getInt(0)).toList
      }
      batchStatements should have size 2
      batchStatements should contain theSameElementsAs Seq(List(1, 3), List(2, 4))

      val stmtss = statements.map {
        case s: BoundStatement => List(s)
        case s: BatchStatement => s.getStatements.map(_.asInstanceOf[BoundStatement]).toList
      }
      stmtss.foreach(stmts => stmts.size should be > 0)
      stmtss.foreach(stmts => if (stmts.size > 1) stmts.map(BoundStatementBuilder.calculateDataSize).sum should be <= 15)
      stmtss.flatten.map(s => (s.getInt(0), s.getString(1))) should contain theSameElementsAs data
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in rows" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(dynamicBatchKeyGen, RowsInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  it should "produce empty stream when no data is available and batch size is specified in bytes" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val data = Seq()
      val statements = bm(dynamicBatchKeyGen, BytesInBatch(10), 1, data.toIterator).toList
      statements should have size 0
    }
  }

  it should "work with random data" in {
    conn.withSessionDo { session =>
      val bm = makeBatchBuilder(session)
      val size = 10000
      val data = (1 to size).map(x => (Random.nextInt().abs, Random.nextString(Random.nextInt(20))))
      val statements = bm(dynamicBatchKeyGen5, RowsInBatch(10), 4, data.toIterator).toList
      val batches = statements.collect { case bs: BatchStatement => bs.size()}
      statements.flatMap {
        case s: BoundStatement => List(s)
        case s: BatchStatement =>
          s.size() should be <= 10
          s.getStatements.map(_.asInstanceOf[BoundStatement])
      }.map(s => (s.getInt(0), s.getString(1))).sortBy(_.toString()) should contain theSameElementsInOrderAs data.sortBy(_.toString())
    }

  }

}
