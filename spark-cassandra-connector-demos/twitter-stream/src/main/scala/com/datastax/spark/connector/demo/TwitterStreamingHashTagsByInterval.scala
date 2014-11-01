package com.datastax.spark.connector.demo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.joda.time.{DateTimeZone, DateTime}
import twitter4j.auth.Authorization
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns

class TwitterStreamingHashTagsByInterval extends Serializable {

  private val columns = SomeColumns("hashtag", "mentions", "interval")

  def now: DateTime = new DateTime(DateTimeZone.UTC)
  def now(pattern: String): String = now.toString(pattern)

  def start(auth: Option[Authorization], ssc: StreamingContext, tags: Set[String], keyspace: String, table: String): Unit = {

    val filter = tags.mkString("|")

    val RegexPattern =
      if (tags.mkString contains "#") s"""(#\\w*(?:$filter)\\w*)""".r
      else s"""(\\w*(?:$filter)\\w*)""".r

    val stream = TwitterUtils.createStream(ssc, auth, Nil, StorageLevel.MEMORY_ONLY_SER_2)

    val transform = (cruft: String) =>
      RegexPattern.findAllIn(cruft).toSeq.map(_.stripPrefix("#"))

    val hashTags = stream
      .flatMap(_.getText.toLowerCase.split("""\s+"""))
      .flatMap(transform)
      .map((_, 1))
      .reduceByKey(_ + _)

    val tagCountsByMinute = hashTags
      .map{case (hashtag, mentions) => (hashtag, mentions, "M" +now("yyyyMMddHHmm"))}

    val tagCountsByHour = hashTags.
      map{case (hashtag, mentions) => (hashtag, mentions, "H" + now("yyyyMMddHH"))}

    val tagCountsByDay  = hashTags
      .map{case (hashtag, mentions) => (hashtag, mentions, "D" + now("yyyyMMdd"))}

    val tagCountsAll    = hashTags
      .map{case (hashtag, mentions) => (hashtag, mentions, "ALL")}


    tagCountsByMinute.saveToCassandra(keyspace, table, columns)
    tagCountsByHour.saveToCassandra(keyspace, table, columns)
    tagCountsByDay.saveToCassandra(keyspace, table, columns)
    tagCountsAll.saveToCassandra(keyspace, table, columns)

    //ssc.checkpoint("./checkpoint")

    ssc.start()
    ssc.awaitTermination()
  }
}


