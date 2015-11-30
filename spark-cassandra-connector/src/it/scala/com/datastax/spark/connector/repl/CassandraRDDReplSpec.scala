package com.datastax.spark.connector.repl

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.embedded._

class CassandraRDDReplSpec extends SparkCassandraITFlatSpecBase with SparkRepl {
  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(null)

  val conn = CassandraConnector(defaultConf)

  conn.withSessionDo { session =>
    createKeyspace(session)

    session.execute(s"CREATE TABLE IF NOT EXISTS $ks.simple_kv (key INT, value TEXT, PRIMARY KEY (key))")
    session.execute(s"INSERT INTO $ks.simple_kv (key, value) VALUES (1, '0001')")
    session.execute(s"INSERT INTO $ks.simple_kv (key, value) VALUES (2, '0002')")
    session.execute(s"INSERT INTO $ks.simple_kv (key, value) VALUES (3, '0003')")
  }

  it should "allow to read a Cassandra table as Array of Scala class objects in REPL" in {
    val output = runInterpreter(
      s"""
        |import com.datastax.spark.connector._
        |
        |case class SampleScalaCaseClass(key: Int, value: String)
        |val cnt1 = sc.cassandraTable[SampleScalaCaseClass]("$ks", "simple_kv").toArray.length
        |
        |class SampleScalaClass(val key: Int, val value: String) extends Serializable
        |val cnt2 = sc.cassandraTable[SampleScalaClass]("$ks", "simple_kv").toArray.length
        |
        |class SampleScalaClassWithNoFields(key: Int, value: String) extends Serializable
        |val cnt3 = sc.cassandraTable[SampleScalaClassWithNoFields]("$ks", "simple_kv").toArray.length
        |
        |class SampleScalaClassWithMultipleCtors(var key: Int, var value: String) extends Serializable {
        |  def this(key: Int) = this(key, null)
        |  def this() = this(0, null)
        |}
        |val cnt4 = sc.cassandraTable[SampleScalaClassWithMultipleCtors]("$ks", "simple_kv").toArray.length
        |
        |class SampleWithNestedScalaCaseClass extends Serializable {
        |  case class InnerClass(key: Int, value: String)
        |}
        |val cnt5 = sc.cassandraTable[SampleWithNestedScalaCaseClass#InnerClass]("$ks", "simple_kv").toArray.length
        |
        |class SampleWithDeeplyNestedScalaCaseClass extends Serializable {
        |  class IntermediateClass extends Serializable {
        |    case class InnerClass(key: Int, value: String)
        |  }
        |}
        |val cnt6 = sc.cassandraTable[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass]("$ks", "simple_kv").toArray.length
        |
        |object SampleObject extends Serializable {
        |  case class ClassInObject(key: Int, value: String)
        |}
        |val cnt7 = sc.cassandraTable[SampleObject.ClassInObject]("$ks", "simple_kv").toArray.length
      """.stripMargin)
    output should not include "error:"
    output should not include "Exception"
    output should include("cnt1: Int = 3")
    output should include("cnt2: Int = 3")
    output should include("cnt3: Int = 3")
    output should include("cnt4: Int = 3")
    output should include("cnt5: Int = 3")
    output should include("cnt6: Int = 3")
    output should include("cnt7: Int = 3")
  }

}
