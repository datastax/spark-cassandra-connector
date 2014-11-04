package com.datastax.spark.connector.demo

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{DateTimeZone, DateTime}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._

/**
 * Set Twitter credentials in your deploy/runtime environment:
 * - see  [[com.datastax.spark.connector.demo.Twitter.TwitterAuth]]
 * export TWITTER_CONSUMER_KEY="value"
 * export TWITTER_CONSUMER_SECRET="value"
 * export TWITTER_ACCESS_TOKEN="value"
 * export TWITTER_ACCESS_TOKEN_SECRET="value"
 *
 * Or pass in as -D system properties:
 * -Dtwitter4j.oauth.consumerKey="value"
 * -Dtwitter4j.oauth.consumerSecret="value"
 * -Dtwitter4j.oauth.accessToken="value"
 * -Dtwitter4j.oauth.accessTokenSecret="value"
 *
 * Other configurable options - see [[TwitterSettings]], and /resources/application.conf
 * -Dspark.master, default is localhost
 * -Dspark.cassandra.connection.host, default is 127.0.0.1
 * -Dspark.cores.max, default configured is 2
 *
 * Verify data persisted after running in cqlsh: should output sequences similar to:
 * {{{
 *    hashtag            | interval      | mentions
 *   --------------------+---------------+----------
 *      android          | M201411011649 |        1
 *      android          | M201411011648 |        9
 *      android          | M201411011647 |        2
 *      android          |   H2014110116 |       12
 *      android          |     D20141101 |       12
 *      android          |           ALL |       12
 * }}}
 */
object TwitterStreamingApp {
  import Twitter._

  val settings = new TwitterSettings
  import settings._

  val conf = new SparkConf(true)
    .setMaster(SparkMaster)
    .setAppName(getClass.getSimpleName)
    .setJars(DeployJars)
    .set("spark.executor.memory", SparkExecutorMemory.toString)
    .set("spark.cores.max", SparkCoresMax.toString)
    .set("spark.cassandra.connection.host", CassandraSeedNodes)

  createSchema()

  val credentials = TwitterAuth()
  if(credentials.auth.isEmpty) throw new IllegalArgumentException(
    s"Twitter Credentials not found in the environment of from System properties")

  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(StreamingBatchInterval))

  def main(args: Array[String]): Unit = {
    val stream = new TwitterStreamingHashTagsByInterval
    stream.start(credentials.auth, ssc, RegexFilterPattern, CassandraKeyspace, CassandraTable)
  }

  /** Creates the keyspace and table schema. */
  def createSchema(): Unit = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS $CassandraKeyspace")
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"""
             CREATE TABLE IF NOT EXISTS $CassandraKeyspace.$CassandraTable (
                hashtag text,
                interval text,
                mentions counter,
                PRIMARY KEY(hashtag, interval)
            ) WITH CLUSTERING ORDER BY (interval DESC)
           """)
    }
  }

  def now: DateTime = new DateTime(DateTimeZone.UTC)
  def now(pattern: String): String = now.toString(pattern)
}
