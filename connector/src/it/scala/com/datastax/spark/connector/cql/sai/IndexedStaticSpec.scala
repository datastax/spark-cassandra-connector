package com.datastax.spark.connector.cql.sai

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.ccm.CcmConfig.DSE_V6_8_3
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._

class IndexedStaticSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.static_test (
             |  pk int,
             |  ck int,
             |  static_col_1 int STATIC,
             |  static_col_2 int STATIC,
             |
             |  PRIMARY KEY (pk, ck));""".stripMargin)

        session.execute(
          s"CREATE CUSTOM INDEX static_sai_idx ON $ks.static_test (static_col_1) USING 'StorageAttachedIndex';")

      }
    }
  }
  "Index on static columns" should {
    "allow for predicate push down" in dseFrom(DSE_V6_8_3) {
      assertPushDown(df("static_test").filter(col("static_col_1") === 1))
    }

    "not cause predicate push down for non-indexed static columns" in dseFrom(DSE_V6_8_3) {
      assertNoPushDown(df("static_test").filter(col("static_col_2") === 1))
    }
  }
}
