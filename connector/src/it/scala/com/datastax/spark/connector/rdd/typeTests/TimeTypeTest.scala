package com.datastax.spark.connector.rdd.typeTests

import java.time.LocalTime

import com.datastax.spark.connector._

import com.datastax.oss.driver.api.core.DefaultProtocolVersion
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class TimeTypeTest extends AbstractTypeTest[LocalTime, LocalTime] with DefaultCluster {

  override val minPV = DefaultProtocolVersion.V4

  override def getDriverColumn(row: Row, colName: String): LocalTime = row.getLocalTime(colName)

  override protected val typeName: String = "time"

  override protected val typeData: Seq[LocalTime] = (1L to 5L).map(LocalTime.ofNanoOfDay)
  override protected val addData: Seq[LocalTime] = (6L to 10L).map(LocalTime.ofNanoOfDay)

  "Time Types" should "be writable as dates" in skipIfProtocolVersionLT(minPV) {
    val times = (100 to 500 by 100).map(LocalTime.ofNanoOfDay(_))
    sc.parallelize(times.map(x => (x, x, x, x))).saveToCassandra(keyspaceName, typeNormalTable)
    val results = sc.cassandraTable[(LocalTime, LocalTime, LocalTime, LocalTime)](keyspaceName, typeNormalTable).collect
    checkNormalRowConsistency(times, results)
  }

}
