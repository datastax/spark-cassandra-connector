package com.datastax.spark.connector.rdd.partitioner

import scala.collection.JavaConversions._

import com.datastax.spark.connector.util.Logging

import com.datastax.driver.core.exceptions.InvalidQueryException
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.dht.{TokenFactory, Token}


/** Estimates amount of data in the Cassandra table.
  * Takes token range size estimates from the `system.size_estimates` table, 
  * available since Cassandra 2.1.5. */
class DataSizeEstimates[V, T <: Token[V]](
    conn: CassandraConnector,
    keyspaceName: String,
    tableName: String)(
  implicit
    tokenFactory: TokenFactory[V, T])
  extends Logging {

  /** Represents a single `system.size_estimates` table row */
  private case class TokenRangeSizeEstimate(
      rangeStart: T,
      rangeEnd: T,
      partitionsCount: Long,
      meanPartitionSize: Long) {

    def ringFraction: Double =
      tokenFactory.ringFraction(rangeStart, rangeEnd)
    
    def totalSizeInBytes: Long = 
      partitionsCount * meanPartitionSize
  }

  private lazy val tokenRanges: Seq[TokenRangeSizeEstimate] =
    conn.withSessionDo { session =>
      try {
        val rs = session.execute(
          "SELECT range_start, range_end, partitions_count, mean_partition_size " +
            "FROM system.size_estimates " +
            "WHERE keyspace_name = ? AND table_name = ?", keyspaceName, tableName)

        for (row <- rs.all()) yield TokenRangeSizeEstimate(
          rangeStart = tokenFactory.tokenFromString(row.getString("range_start")),
          rangeEnd = tokenFactory.tokenFromString(row.getString("range_end")),
          partitionsCount = row.getLong("partitions_count"),
          meanPartitionSize = row.getLong("mean_partition_size")
        )

        // The table may not contain the estimates yet if the data was just inserted and the
        // amount of data in the table was small. This is very common situation during tests,
        // when we insert a few rows and immediately query them. However, for tiny data sets the lack
        // of size estimates is not a problem at all, because we don't want to split tiny data anyways.
        // Therefore, we're not issuing a warning if the result set was empty.
      }
      catch {
        case e: InvalidQueryException =>
          logError(
            s"Failed to fetch size estimates for $keyspaceName.$tableName from system.size_estimates " +
              s"table. The number of created Spark partitions may be inaccurate. " +
              s"Please make sure you use Cassandra 2.1.5 or newer.", e)
          Seq.empty
      }
    }

  private lazy val ringFraction = 
    tokenRanges.map(_.ringFraction).sum

  /** Estimates the total number of partitions in a ring */
  lazy val partitionCount: Long = {
    val partitionsCount = tokenRanges.map(_.partitionsCount).sum
    val normalizedCount = (partitionsCount / ringFraction).toLong
    logDebug(s"Estimated partition count of $keyspaceName.$tableName is $normalizedCount")
    normalizedCount
  }

  /** Estimates the total amount of data in a table assuming no replication. */
  lazy val dataSizeInBytes: Long = {
    val tokenRangeSizeInBytes = (totalDataSizeInBytes / ringFraction).toLong
    logDebug(s"Estimated size of $keyspaceName.$tableName is $tokenRangeSizeInBytes bytes")
    tokenRangeSizeInBytes
  }

  /** Estimates the total amount of data in a table without normalization assuming no replication. */
  lazy val totalDataSizeInBytes: Long = {
    tokenRanges.map(_.totalSizeInBytes).sum
  }
}

object DataSizeEstimates {

  /** Waits until data size estimates are present in the system.size_estimates table.
    * Returns true if size estimates were written, returns false if timeout was reached
    * while waiting */
  def waitForDataSizeEstimates(
    conn: CassandraConnector,
    keyspaceName: String,
    tableName: String,
    timeoutInMs: Int): Boolean = {

    conn.withSessionDo { session =>
      def hasSizeEstimates: Boolean = {
        session.execute(
          s"SELECT * FROM system.size_estimates " +
            s"WHERE keyspace_name = '$keyspaceName' AND table_name = '$tableName'").all().nonEmpty
      }

      val startTime = System.currentTimeMillis()
      while (!hasSizeEstimates && System.currentTimeMillis() < startTime + timeoutInMs)
        Thread.sleep(1000)

      hasSizeEstimates
    }
  }
}
