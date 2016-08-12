package org.apache.spark.sql.cassandra

import com.google.common.reflect.ClassPath
import org.apache.spark.sql.sources.Filter
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions

class AliasedCassandraSourceRelationSpec extends FlatSpec with Matchers {
  it should "know all of the Filter implementations distributed with Spark" in {

    val classInfos = ClassPath
      .from(this.getClass.getClassLoader)
      .getTopLevelClasses("org.apache.spark.sql.sources")
    val filters = JavaConversions.asScalaSet(classInfos)
      .map(_.load())
      .filter(_.getSuperclass == classOf[Filter])

    withClue(
      s"""Aliasing functionality should know all filters distributed with spark.
        | If this test fails than probably you have upgraded Spark distro and now you need to teach
        | aliasing functionality how to deal with new filters (added in upgraded distro).
        | Go to ${classOf[AliasedCassandraSourceRelation].getCanonicalName} and check what filters you
        | are missing. Than adjust this test's value below. Here is a list of found filters:
        | ${filters.map(_.getCanonicalName).mkString("\n")}
      """.stripMargin) {
      filters.size should be(15)
    }
  }
}
