package com.datastax.spark.connector.demo

import scala.concurrent.duration._
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{DateTimeZone, DateTime}
import twitter4j.auth.{OAuthAuthorization, Authorization}
import twitter4j.conf.ConfigurationBuilder
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
/*
  Runtime.getRuntime.addShutdownHook(new Thread("Shutdown") {
    override def run() {
      ssc.stop(true, false)
      Thread.sleep(3000)
    }
  })*/

  def main(args: Array[String]): Unit = {
    val stream = new TwitterStreamingHashTagsByInterval
    stream.start(credentials.auth, ssc, filters, CassandraKeyspace, CassandraTable)
  }

  /** Creates the keyspace and table schema. */
  def createSchema(): Unit = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS $CassandraKeyspace")
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"""
             CREATE TABLE IF NOT EXISTS $CassandraKeyspace.$CassandraTable (
                hashtag text,
                mentions counter,
                interval text,
                PRIMARY KEY(hashtag, interval)
            ) WITH CLUSTERING ORDER BY (interval DESC)
           """)
    }
  }

  def now: DateTime = new DateTime(DateTimeZone.UTC)
  def now(pattern: String): String = now.toString(pattern)
}

object Twitter {

  case class TwitterAuth(auth: Option[Authorization])

  object TwitterAuth {

    /** Creates an instance of TwitterAuth by first attempting to acquire
      * from the deploy environment variables.
      *
      * If the settings exist in the deploy environment, and if not,
      * falls back to acquiring from java system properties passed in.
      *
      * Auth settings allow the Twitter4j library, used by twitter stream,
      * to generate OAuth credentials. */
    def apply(): TwitterAuth = {
      val args = Array(
        sys.env.getOrElse("TWITTER_CONSUMER_KEY", sys.props("twitter4j.oauth.consumerKey")),
        sys.env.getOrElse("TWITTER_CONSUMER_SECRET", sys.props("twitter4j.oauth.consumerSecret")),
        sys.env.getOrElse("TWITTER_ACCESS_TOKEN", sys.props("twitter4j.oauth.accessToken")),
        sys.env.getOrElse("TWITTER_ACCESS_TOKEN_SECRET", sys.props("twitter4j.oauth.accessTokenSecret")))

      val opt = if (args.contains(null)) None else Some(toAuth(args(0),args(1),args(2),args(3)))
      TwitterAuth(opt)
    }

    /** Auth settings allow the Twitter4j library, used by twitter stream, to generate OAuth credentials. */
    def toAuth(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String): Authorization =
      new OAuthAuthorization(new ConfigurationBuilder()
        .setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret)
        .build)
  }
}