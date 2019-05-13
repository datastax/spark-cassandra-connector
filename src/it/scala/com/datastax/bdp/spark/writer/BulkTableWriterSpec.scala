/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.spark.writer

import java.io.IOException
import java.nio.file.Paths

import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD

import com.datastax.bdp.config.YamlClientConfiguration
import com.datastax.bdp.spark.writer.BulkTableWriter._
import com.datastax.bdp.test.ng.YamlProvider
import com.datastax.spark.connector.DseITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, YamlTransformations}

class BulkTableWriterSpec extends DseITFlatSpecBase {

  YamlClientConfiguration.setAsClientConfigurationImpl()

  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(sparkConf)

  YamlProvider.provideCassandraYaml(EmbeddedCassandra.runner(0).confFile.toPath)

  override lazy val conn = CassandraConnector(sparkConf)

  "BulkTableWriter" should "save RDD to Cassandra" in {
    conn.withSessionDo { session =>

      session.execute("CREATE KEYSPACE IF NOT EXISTS bulk_writer_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE bulk_writer_test.test_1(pkey int PRIMARY KEY, value varchar)")

      val count = 1000
      val rdd: RDD[(Int, String)] = sc.parallelize(
        for (i <- 1 to count) yield (i, "value" + i))

      rdd.bulkSaveToCassandra("bulk_writer_test", "test_1")

      val res1 = session.execute("SELECT count(*) FROM bulk_writer_test.test_1")
      res1.one().getLong(0) should be(count)

      val res2 = session.execute("SELECT * FROM bulk_writer_test.test_1")
      var receivedKeys = Set.empty[Int]
      for (row <- res2.all()) {
        val key = row.getInt(0)
        key should be >= 1
        key should be <= count
        row.getString(1) should startWith("value")
        receivedKeys += key
      }
      receivedKeys should have size count
    }
  }

  it should "throw an exception if the table is not found" in {
    val rdd = sc.emptyRDD[(Int, String)]
    an[IOException] should be thrownBy rdd.bulkSaveToCassandra("bulk_writer_test", "test_2")
  }

  it should "throw an exception containing output directory name if the output directory could not be created" in {
    conn.withSessionDo { session =>

      session.execute("CREATE KEYSPACE IF NOT EXISTS bulk_writer_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE bulk_writer_test.test_3(pkey int PRIMARY KEY, value varchar)")

      val count = 1000
      val rdd: RDD[(Int, String)] = sc.parallelize(
        for (i <- 1 to count) yield (i, "value" + i))

      val outputDir = Paths.get("/proc/invalid_directory") // even root cannot write under /proc directory
    val writeConf = BulkWriteConf(outputDirectory = Some(outputDir))
      val exception = the[Exception] thrownBy
        rdd.bulkSaveToCassandra("bulk_writer_test", "test_3", writeConf = writeConf)
      exception.getMessage should include(outputDir.toString)
    }
  }
}
