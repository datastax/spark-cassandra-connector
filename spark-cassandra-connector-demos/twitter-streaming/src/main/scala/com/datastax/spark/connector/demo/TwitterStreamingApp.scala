package com.datastax.spark.connector.demo

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector

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
 * Verify data persisted after running in cqlsh with:
 * cqlsh> select * from twitter_stream.topics_by_interval;
 * 
 * You should output sequences similar to:
 * {{{
 *   topic | interval             | mentions
 *  -------+----------------------+----------
 *     cat | 2014122415:08:20.000 |    1
 *     cat | 2014122415:06:55.000 |    1
 *     cat | 2014122415:06:50.000 |    2
 *     cat | 2014122415:06:10.000 |    1
 *     dog | 2014122415:08:10.000 |    1
 *     dog | 2014122415:08:05.000 |    1
 *     dog | 2014122415:07:10.000 |    3
 *     dog | 2014122415:06:15.000 |    1
 * }}}
 *
 * Note that Cassandra is ordering each topic by interval, from most recent,
 * versus having Spark order data.
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

  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(StreamingBatchInterval))

  def main(args: Array[String]): Unit = {
    val stream = new TwitterStreamingTopicsByInterval
    stream.start(credentials.auth, ssc, Topics, CassandraKeyspace, CassandraTable)
  }

  /** Creates the keyspace and table schema. */
  def createSchema(): Unit = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS $CassandraKeyspace")
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"""
             CREATE TABLE IF NOT EXISTS $CassandraKeyspace.$CassandraTable (
                topic text,
                interval text,
                mentions counter,
                PRIMARY KEY(topic, interval)
            ) WITH CLUSTERING ORDER BY (interval DESC)
           """)
    }
  }
}
