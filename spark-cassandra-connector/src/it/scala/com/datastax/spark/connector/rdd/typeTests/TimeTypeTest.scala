package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.Row
import com.datastax.spark.connector._
import java.util.Date

class TimeTypeTest extends AbstractTypeTest[Long, java.lang.Long] {

  override def getDriverColumn(row: Row, colName: String): Long = row.getTime(colName)

  override protected val typeName: String = "time"

  override protected val typeData: Seq[Long] = 1L to 5L
  override protected val typeSet: Set[Long] = (1L to 3L).toSet
  override protected val typeMap1: Map[String, Long] = Map("key1" -> 1L, "key2" -> 2L, "key3" -> 3L)
  override protected val typeMap2: Map[Long, String] = Map(1L -> "val1", 2L -> "val2", 3L -> "val3")

  override protected val addData: Seq[Long] = 6L to 10L
  override protected val addSet: Set[Long] = (4L to 6L).toSet
  override protected val addMap1: Map[String, Long] = Map("key4" -> 4L, "key5" -> 5L, "key3" -> 6L)
  override protected val addMap2: Map[Long, String] = Map(4L -> "val4", 5L -> "val5", 6L -> "val6")

  "Time Types" should "be writable as dates" in {
    val dates = (100 to 500 by 100).map(new Date(_))
    val times = dates.map(_.getTime)
    sc.parallelize(
      dates.map(x => (x,x,x))
    ).saveToCassandra(keyspaceName, "time_compound")
    val results = sc.cassandraTable[(Long, Long, Long)](keyspaceName, "time_compound").collect
    checkNormalRowConsistency(times.map(_ * 1000000), results)
  }

  /*
  Todo Determine a way to detect when a column is being read as a Long and it is an underlying Time
  type. This needs a special conversion since a normal Long will convert into milliseconds rather than
  nanoseconds.
   */
  ignore should "be readable as dates" in {
    val dates = (100 to 500 by 100).map(new Date(_))
    val times = dates.map(_.getTime)
    val results = sc.cassandraTable[(Date, Date, Date)](keyspaceName, "time_compound").collect
    val resultsAsLong = results.map{ case (x,y,z) => (x.getTime, y.getTime, z.getTime)}
    checkNormalRowConsistency(times, resultsAsLong)
  }

}
