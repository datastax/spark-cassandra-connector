package com.datastax.spark.connector.demo

import scala.util.matching.Regex
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.joda.time.{DateTimeZone, DateTime}
import twitter4j.auth.Authorization
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns

class TwitterStreamingHashTagsByInterval extends Serializable {

  def now: DateTime = new DateTime(DateTimeZone.UTC)
  def now(pattern: String): String = now.toString(pattern)

  def start(auth: Option[Authorization], ssc: StreamingContext, filters: Regex, keyspace: String, table: String): Unit = {

    val transform = (cruft: String) =>
      filters.findAllIn(cruft).toSeq.map(_.stripPrefix("#"))

    val stream = TwitterUtils.createStream(ssc, auth, Nil, StorageLevel.MEMORY_ONLY_SER_2)

    val terms = stream.flatMap(_.getText.toLowerCase.split("""\s+""")).flatMap(transform).map((_, 1))

    /** Note that Cassandra is doing the sorting for you here because we set the schema to
      * {{{ WITH CLUSTERING ORDER BY (interval DESC) }}}
      * Thus, no `sortByKey` necessary.
      */
    val minute = terms.reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (term, count) => (term, count, "M" + now("yyyyMMddHHmm"))}
      .saveToCassandra(keyspace, table, SomeColumns("hashtag", "mentions", "interval"))

    val hour = terms.reduceByKeyAndWindow(_ + _, Seconds(3600))
      .map{case (term, count) => (term, count, "H" + now("yyyyMMddHH"))}
      .saveToCassandra(keyspace, table, SomeColumns("hashtag", "mentions", "interval"))

    ssc.start()
    ssc.awaitTermination()
  }
}


