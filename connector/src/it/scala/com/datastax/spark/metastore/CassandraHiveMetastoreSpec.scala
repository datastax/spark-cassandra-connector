package com.datastax.spark.metastore

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

import scala.concurrent.Future

class CassandraHiveMetastoreSpec extends SparkCassandraITFlatSpecBase with DefaultCluster {

  override lazy val conn = CassandraConnector(defaultConf)

  val COLORS_TXT =
    """1\u0001red
      |2\u0001blue
      |3\u0001green
      |4\u0001yellow
      |5\u0001orange
      |6\u0001black
      |7\u0001brown
      |8\u0001gray
      |9\u0001purple
      |10\u0001white""".stripMargin

  override def beforeClass {

    conn.withSessionDo { session =>
      createKeyspace(session, ks)
      awaitAll {
        Future {

          session.execute(s"CREATE table $ks.basic (k int, c int, v int, PRIMARY KEY (k,c))")
        }
        Future {
          val cols = (1 to 1000).map( index => s"col_$index int").mkString(", ")
          session.execute(s"CREATE TABLE $ks.manycolumns ( k int, $cols, PRIMARY KEY (k))")
        }
        Future{
          val tmpDir = Files.createTempDirectory("foo")
          val colors_txt = Paths.get(tmpDir.toString, "colors.txt")
          Files.write(colors_txt, COLORS_TXT.getBytes(StandardCharsets.UTF_8))
          sparkSession.sql("CREATE TABLE IF NOT EXISTS test_hive (id INT, color STRING) PARTITIONED BY (ds STRING)")
          sparkSession.sql(s"LOAD DATA INPATH 'file://$colors_txt' OVERWRITE INTO TABLE test_hive PARTITION (ds ='2008-08-15')")
        }
      }
    }
  }

  "CassandraHiveMetastore" should "auto create reference to a basic table" in {
    sparkSession.sql(s"SELECT * FROM $ks.basic").schema.length should be (3)
  }


  it should "auto create reference to a very large table" in {
    sparkSession.sql(s"SELECT * FROM $ks.manycolumns").schema.length should be (1001)
  }

  it should "be able to read from a hive loaded table" in {
    sparkSession.sql("SELECT * FROM test_hive").count() should be (10)
  }



}
