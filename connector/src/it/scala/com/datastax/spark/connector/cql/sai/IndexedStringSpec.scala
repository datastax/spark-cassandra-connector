package com.datastax.spark.connector.cql.sai

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.ccm.CcmConfig.DSE_V6_8_3
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._


class IndexedStringSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  private val stringTypes = Seq("text", "ascii", "varchar", "uuid")

  override def beforeClass {
    dseFrom(DSE_V6_8_3) {
      conn.withSessionDo { session =>

        createKeyspace(session)
        createTableWithIndexes(session, "text_types_test", stringTypes)

        for (i <- 0 to 9) {
          session.execute(s"insert into $ks.text_types_test (pk, text_col, ascii_col, varchar_col, uuid_col) " +
            s"values ($i, 'text$i', 'text$i', 'text$i', 123e4567-e89b-12d3-a456-42661417400$i)")
        }
      }
    }
  }

  private def indexOnStringColumn(columnName: String): Unit = {
    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("text_types_test").filter(col(columnName) === "text3")
      assertPushedPredicate(data, pushedPredicate = EqualTo(columnName, "text3"))
      data.count() shouldBe 1
    }

    "not allow for contains predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("text_types_test").filter(col(columnName) contains "text5")
      assertNoPushDown(data)
      data.count() shouldBe 1
    }

    "not allow for range predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("text_types_test").filter(col(columnName) < "text4")
      assertNoPushDown(data)
      data.count() shouldBe 4
    }

    "allow only for a single equality push down if there are more than one" in dseFrom(DSE_V6_8_3) {
      val data = df("text_types_test").filter(col(columnName) === "text1" and col(columnName) === "text2")
      assertPushedPredicate(data, pushedPredicate = EqualTo(columnName, "text1"))
      data.count() shouldBe 0
    }
  }

  "Index on a text column" should {
    indexOnStringColumn("text_col")
  }

  "Index on a ascii column" should {
    indexOnStringColumn("ascii_col")
  }

  "Index on a varchar column" should {
    indexOnStringColumn("varchar_col")
  }

  "Index on a uuid column" should {
    "allow for equality predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("text_types_test").filter(col("uuid_col") === "123e4567-e89b-12d3-a456-426614174003")
      assertPushedPredicate(data, pushedPredicate = EqualTo("uuid_col", "123e4567-e89b-12d3-a456-426614174003"))
      data.count() shouldBe 1
    }

    "not allow for range predicate push down" in dseFrom(DSE_V6_8_3) {
      val data = df("text_types_test").filter(col("uuid_col") < "123e4567-e89b-12d3-a456-426614174004")
      assertNoPushDown(data)
      data.count() shouldBe 4
    }
  }

}
