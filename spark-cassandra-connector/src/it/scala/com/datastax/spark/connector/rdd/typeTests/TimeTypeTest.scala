package com.datastax.spark.connector.rdd.typeTests

import com.datastax.driver.core.Row
import com.datastax.spark.connector._
import java.util.Date

class TimeTypeTest extends AbstractTypeTest[Long, java.lang.Long] {

  override def getDriverColumn(row: Row, colName: String): Long = row.getTime(colName)

  override protected val typeName: String = "time"

  override protected val typeData: Seq[Long] = 1L to 5L
  override protected val addData: Seq[Long] = 6L to 10L

  "Time Types" should "be writable as dates" in {
    val dates = (100 to 500 by 100).map(new Date(_))
    val times = dates.map(_.getTime)
    sc.parallelize(
      dates.map(x => (x, x, x, x))
    ).saveToCassandra(keyspaceName, typeNormalTable)
    val results = sc.cassandraTable[(Long, Long, Long, Long)](keyspaceName, typeNormalTable).collect
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
    val results = sc.cassandraTable[(Date, Date, Date, Date)](keyspaceName, "time_compound").collect
    val resultsAsLong = results.map{ case (x, y, z, a) => (x.getTime, y.getTime, z.getTime, a.getTime)}
    checkNormalRowConsistency(times, resultsAsLong)
  }

}
