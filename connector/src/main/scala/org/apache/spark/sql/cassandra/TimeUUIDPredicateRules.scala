/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.types.TimeUUIDType
import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.PredicateOps.FilterOps
import org.apache.spark.sql.sources.Filter

/** All non-equal predicates on a TimeUUID column are going to fail and fail
  * in silent way. The basic issue here is that when you use a comparison on
  * a time UUID column in C* it compares based on the Time portion of the UUID. When
  * Spark executes this filter (unhandled behavior) it will compare lexically, this
  * will lead to results being incorrectly filtered out of the set. As long as the
  * range predicate is handled completely by the connector the correct result
  * will be obtained.
  */
object TimeUUIDPredicateRules extends CassandraPredicateRules with Logging {

  private def isTimeUUIDNonEqualPredicate(tableDef: TableDef, predicate: Filter): Boolean = {
    if (FilterOps.isSingleColumnPredicate(predicate)) {
      // extract column name only from single column predicates, otherwise an exception is thrown
      val columnName = FilterOps.columnName(predicate)
      val isRange = FilterOps.isRangePredicate(predicate)
      val isTimeUUID = tableDef.columnByName.get(columnName).exists(_.columnType == TimeUUIDType)
      isRange && isTimeUUID
    } else {
      false
    }
  }

  override def apply(predicates: AnalyzedPredicates, tableDef: TableDef, conf: SparkConf): AnalyzedPredicates = {
    val unhandledTimeUUIDNonEqual = predicates.handledBySpark.filter(isTimeUUIDNonEqualPredicate(tableDef, _))
    require(unhandledTimeUUIDNonEqual.isEmpty,
      s"""
         | You are attempting to do a non-equality comparison on a TimeUUID column in Spark.
         | Spark can only compare TimeUUIDs Lexically which means that the comparison will be
         | different than the comparison done in C* which is done based on the Time Portion of
         | TimeUUID. This will in almost all cases lead to incorrect results. If possible restrict
         | doing a TimeUUID comparison only to columns which can be pushed down to Cassandra.
         | https://datastax-oss.atlassian.net/browse/SPARKC-405.
         |
         | $unhandledTimeUUIDNonEqual
    """.stripMargin)

    predicates
  }
}
