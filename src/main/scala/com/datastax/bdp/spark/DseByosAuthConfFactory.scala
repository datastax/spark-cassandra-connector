/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.spark

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil

import com.datastax.bdp.cassandra.auth.DseJavaDriverAuthProvider
import com.datastax.bdp.config.{AbstractPropertyBasedClientConfiguration, ClientConfiguration}
import com.datastax.bdp.spark.DseAuthConfFactory.{CassandraPasswordProperty, CassandraUserNameProperty}
import com.datastax.bdp.transport.client.{HadoopBasedClientConfiguration, MapBasedClientConfiguration}
import com.datastax.bdp.transport.server.DigestAuthUtils
import com.datastax.bdp.util.DseConnectionUtil
import com.datastax.driver.core.{AuthProvider, Session}
import com.datastax.driver.dse.auth.{DseGSSAPIAuthProvider, DsePlainTextAuthProvider}
import com.datastax.spark.connector.cql.{AuthConf, AuthConfFactory}
import com.datastax.spark.connector.util.Logging

/**
  * Read client configuration from spark properties and standard hadoop config files
  */
object DseByosAuthConfFactory extends AuthConfFactory with Logging {

  val tokenProperty = "spark.hadoop.cassandra.auth.token"
  val cacheExpirationProperty = "spark.hadoop.cassandra.auth.cache.expiration"

  override def properties: Set[String] = DseAuthConfFactory.properties + tokenProperty

  /** Caches AuthConfs for [[DseByosAuthConfFactory.cacheExpirationProperty]] millis. */
  private class AuthConfCache {

    private class SparkConfCacheKey(val sparkConf: SparkConf) {

      private lazy val debugString = sparkConf.toDebugString

      override def hashCode(): Int = debugString.hashCode

      override def equals(obj: scala.Any): Boolean = obj match {
        case other: SparkConfCacheKey =>
          hashCode == other.hashCode && this.debugString == other.debugString
        case _ => false
      }
    }

    private var confCache: Option[LoadingCache[SparkConfCacheKey, AuthConf]] = None

    private val loader = new CacheLoader[SparkConfCacheKey, AuthConf] {

      override def load(key: SparkConfCacheKey): AuthConf = {
        val conf = key.sparkConf
        val config = new DseByosClientConfiguration(conf)

        //set kerberos protocol system property
        val credentials = for (username <- conf.getOption(CassandraUserNameProperty);
                               password <- conf.getOption(CassandraPasswordProperty)) yield (username, password)

        val token = credentials match {
          case Some((_, _)) =>
            // don't want to even try to get the token if we are not going to use it
            None
          case _ =>
            Option(config.getSaslProtocolName)
              .foreach(System.setProperty(DseGSSAPIAuthProvider.SASL_PROTOCOL_NAME_PROPERTY, _))
            conf.getOption(tokenProperty).orElse(generateDelegateToken(config))
        }
        new ByosAuthConf(config, token, credentials)
      }
    }

    private def initialize(conf: SparkConf): LoadingCache[SparkConfCacheKey, AuthConf] = {
      val expirationAfterWriteMillis = conf.getInt(cacheExpirationProperty, 30 * 60 * 1000)
      val cache = CacheBuilder.newBuilder()
        .expireAfterWrite(expirationAfterWriteMillis, TimeUnit.MILLISECONDS)
        .maximumSize(100)
        .build[SparkConfCacheKey, AuthConf](loader)
      confCache = Some(cache)
      cache
    }

    /** Not thread safe. */
    def get(conf: SparkConf): AuthConf = {
      confCache.getOrElse(initialize(conf)).get(new SparkConfCacheKey(conf))
    }
  }

  /**
    * cache AuthConf cache, to return the same object for the same SparkConf
    * SparkConf has no equals method so we use conf.toDebugString output as a dump of all properties.
    */
  private val confCache = new AuthConfCache()

  override def authConf(conf: SparkConf): AuthConf = synchronized {
    confCache.get(conf)
  }

  /** All tokens issued by this AuthConfFactory. Tokens are canceled on shutdown. */
  private val registeredTokens = mutable.ArrayBuffer[(String, ClientConfiguration)]()

  private def deregisterToken(token: String, session: Session): Unit = {
    try {
      DigestAuthUtils.cancelToken(session, token)
    } catch {
      case t: Throwable => logInfo("Could not cancel token", t)
    }
  }

  private def deregisterTokens(): Unit = synchronized {
    registeredTokens.groupBy(_._2).foreach { case (config, tokens) =>
      val cluster = DseConnectionUtil.createCluster(config, null, null, null)
      try {
        val session = cluster.connect()
        tokens.foreach { case (t, _) => deregisterToken(t, session) }
      } catch {
        case t: Throwable => logInfo("Could not connect to DSE to cancel BYOS tokens", t)
      } finally {
        cluster.closeAsync()
      }
    }
  }

  //Do our best to cancel the token on shutdown, do not relay on Spark event system.
  Runtime.getRuntime.addShutdownHook(new Thread("ByoS Shutdown") {
    override def run(): Unit = deregisterTokens()
  })

  private def registerToken(token: String, config: ClientConfiguration): Unit =
    registeredTokens.append((token, config))

  private def generateDelegateToken(clientConfig: ClientConfiguration): Option[String] = {
    if (clientConfig.isKerberosEnabled) {
      val token = {
        val cluster = DseConnectionUtil.createCluster(clientConfig, null, null, null)
        try {
          val renewer = UserGroupInformation.getCurrentUser.getShortUserName
          DigestAuthUtils.getEncodedToken(cluster.connect(), renewer)
        } finally {
          cluster.closeAsync()
        }
      }
      registerToken(token, clientConfig)
      Some(token)
    } else
      None
  }

  case class ByosAuthConf(
      clientConfig: ClientConfiguration,
      tokenStr: Option[String] = None,
      credentials: Option[(String, String)] = None) extends AuthConf {

    override def authProvider: AuthProvider = {
      (credentials, tokenStr) match {
        case (Some((username, password)), _) =>
          new DsePlainTextAuthProvider(username, password)

        case (_, Some(_tokenStr)) =>
          val token = new Token[TokenIdentifier]()
          token.decodeFromUrlString(_tokenStr)
          new DseJavaDriverAuthProvider(clientConfig, token)

        case _ =>
          AuthProvider.NONE
      }
    }
  }

  class DseByosClientConfiguration(sparkConf: SparkConf)
    extends AbstractPropertyBasedClientConfiguration with ClientConfiguration with Serializable{

    // make it serializable
    val clientConf = new MapBasedClientConfiguration(AbstractPropertyBasedClientConfiguration.configAsMap(
      new HadoopBasedClientConfiguration(SparkHadoopUtil.get.newConfiguration(sparkConf))), "")

    override def get(key: String) = {
      val value = clientConf.get(key)
      if (value != null) value else sparkConf.get(key, null)
    }

    override def getCassandraHosts = {
      val hostString = get("spark.cassandra.connection.host")
      if (StringUtils.isBlank(hostString)) {
        val cassandraHost = getCassandraHost
        if (cassandraHost != null) Array[InetAddress](cassandraHost) else Array.empty
      } else {
        hostString.split(",").map(_.trim).map(InetAddress.getByName(_))
      }
    }

    override def hashCode: Int = clientConf.hashCode()

    override def equals(o: Any): Boolean = {
      if (o == null || (getClass != o.getClass)) return false
      val that = o.asInstanceOf[DseByosClientConfiguration]
      that.clientConf.equals(this.clientConf)
    }
  }

  // for test use
  def getDseByosClientConfiguration(conf: SparkConf): DseByosClientConfiguration = {
    new DseByosClientConfiguration(conf)
  }
}
