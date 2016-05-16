package com.datastax.spark.connector.rdd

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.embedded.SparkTemplate
import com.datastax.spark.connector.rdd.CassandraJoinRDDBenchmark._

/**
  * Performs a benchmark of fetching and joining C* table data with given "left" iterator. This
  * is used in [[RDDFunctions.joinWithCassandraTable]] operation.
  * External C* cluster should be started before benchmark execution.
  */
@State(Scope.Benchmark)
class CassandraJoinRDDBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx2048m"))
  def measureOneElementPerPartitionsJoin(f: OneElementPerPartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx2048m"))
  def measureSmallPartitionsJoin(f: SmallPartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx2048m"))
  def measureModeratePartitionsJoin(f: ModeratePartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(jvmArgs = Array("-Xmx2048m"))
  def measureBigPartitionsJoin(f: BigPartitionsFixture): List[_] = {
    val it = f.rdd.fetchIterator(f.session, f.rdd.boundStatementBuilder(f.session), f.left.iterator)
    it.toList
  }
}

object CassandraJoinRDDBenchmark {
  val Keyspace = "spark_connector_join_performance_test"

  val Rows = 200000
  val SmallPartitionSize = 10
  val ModeratePartitionSize = 500
  val BigPartitionSize = 5000

  @State(Scope.Benchmark)
  class Fixture(partitionSize: Int, table: String) extends SparkTemplate {

    def NormalState() {}

    val config = SparkTemplate.defaultConf.set("spark.cassandra.connection.port", "9042")
    useSparkConf(config)

    var left: Seq[Tuple1[Int]] = _
    var rdd: CassandraJoinRDD[Tuple1[Int], CassandraRow] = _
    var session: Session = _

    @Setup(Level.Trial)
    def init(): Unit = {
      left = (0 until Rows / partitionSize).map(Tuple1(_))
      rdd = sc.parallelize(left).joinWithCassandraTable(Keyspace, table)
      session = rdd.connector.openSession()
    }

    @TearDown(Level.Trial)
    def tearDown(): Unit = {
      session.close()
      sc.stop()
    }
  }

  @State(Scope.Benchmark)
  class OneElementPerPartitionsFixture extends Fixture(1, "one_element_partitions")

  @State(Scope.Benchmark)
  class SmallPartitionsFixture extends Fixture(SmallPartitionSize, "small_partitions")

  @State(Scope.Benchmark)
  class ModeratePartitionsFixture extends Fixture(ModeratePartitionSize, "moderate_partitions")

  @State(Scope.Benchmark)
  class BigPartitionsFixture extends Fixture(BigPartitionSize, "big_partitions")

}