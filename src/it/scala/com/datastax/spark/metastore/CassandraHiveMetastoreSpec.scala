package com.datastax.spark.metastore

import com.datastax.driver.core.ProtocolVersion._
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.YamlTransformations
import org.apache.spark.sql.cassandra._

import scala.concurrent.Future

class CassandraHiveMetastoreSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(metastoreConf)

  override val conn = CassandraConnector(defaultConf)

  beforeClass {

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
      }
    }
  }

  "CassandraHiveMetastore" should "auto create reference to a basic table" in {
    sparkSession.sql(s"SELECT * FROM $ks.basic").schema.length should be (3)
  }


  it should "auto create reference to a very large table" in {
    sparkSession.sql(s"SELECT * FROM $ks.manycolumns").schema.length should be (1001)
  }



}
