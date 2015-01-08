package com.datastax.spark.connector.demo

import scala.util.matching.Regex
import akka.japi.Util.immutableSeq
import com.typesafe.config.ConfigFactory

final class TwitterSettings {

  protected val config = ConfigFactory.load.getConfig("streaming-app")

  val Topics = immutableSeq(config.getStringList("filters")).toSet

  /** For purposes of live demos #term or term is configurable, as #term may be less frequent. */
  val RegexFilterPattern: Regex = {
    val topics = Topics.mkString("|")
    if (topics contains "#")  s"(#\\w*(?:$topics)\\w*)".r else s"(w*(?:$topics)w*)".r
  }

  /** Attempts to detect System property, falls back to config. */
  val SparkMaster: String = sys.props.get("spark.master").getOrElse(config.getString("spark.master"))

  val StreamingBatchInterval = config.getInt("spark.streaming.batch.interval")

  val SparkExecutorMemory = config.getBytes("spark.executor.memory")

  val SparkCoresMax = sys.props.get("spark.cores.max").getOrElse(config.getInt("spark.cores.max"))

  val DeployJars: Seq[String] = immutableSeq(
    config.getStringList("spark.jars")).filter(new java.io.File(_).exists)

  /** Attempts to detect System property, falls back to config,
    * to produce a comma-separated string of hosts. */
  val CassandraSeedNodes: String = sys.props.get("spark.cassandra.connection.host") getOrElse
        immutableSeq(config.getStringList("spark.cassandra.connection.host")).mkString(",")

  val CassandraKeyspace: String = config.getString("spark.cassandra.keyspace")

  val CassandraTable: String = config.getString("spark.cassandra.table")

}

object Twitter {

  import twitter4j.auth.{OAuthAuthorization, Authorization}
  import twitter4j.conf.ConfigurationBuilder

  case class TwitterAuth(auth: Option[Authorization]) extends Serializable

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
