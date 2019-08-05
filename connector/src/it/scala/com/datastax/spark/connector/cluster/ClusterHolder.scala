package com.datastax.spark.connector.cluster

import com.datastax.spark.connector.ccm.CcmBridge
import com.datastax.spark.connector.util.{Logging, SerialShutdownHooks}
import org.apache.commons.lang3.ClassUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Source of [[Cluster]]s for all tests executed within a single test group (single process/JVM).
  *
  * Since tests are grouped by a cluster ([[Fixture]]) and each group is executed in a separate process,
  * this cache should never have more than one element. Retrieving a different element from the one that is already
  * present in this cache results in extra time (teardown and bootstrap) wasted. Warning is logged when
  * this kind of situation is detected. */
object ClusterHolder extends Logging {

  SerialShutdownHooks.add("Connector test cluster cache shutdown hook", 100)(() => {
    close()
  })

  private val clusters: mutable.Map[String, Seq[Cluster]] = mutable.Map()

  private def close(): Unit = synchronized {
    for (testClusters <- clusters.values; cluster <- testClusters) {
      try {
        cluster.ccmBridge.close()
      } catch {
        case t: Throwable => logWarning(s"Closing a ${cluster.name} failed", t)
      }
    }
  }

  def get(fixture: Fixture): Seq[Cluster] = {
    val key = ClassUtils.getAllInterfaces(fixture.getClass).asScala
      .filter(_.getPackage.equals(classOf[Fixture].getPackage))
      .map(_.getCanonicalName)
      .sorted
      .mkString("_")

    synchronized {
      clusters.getOrElseUpdate(key, {
        if (clusters.nonEmpty) {
          logWarning("Test group should contain only tests with the same cluster fixture. Verify your test group setup. " +
            s"Stopping previous clusters and bootstrapping the new one for ${fixture.getClass.getCanonicalName}")
          for (testClusters <- clusters.values; cluster <- testClusters) {
            cluster.ccmBridge.close()
          }
          clusters.clear()
        }

        fixture.configs.zipWithIndex.map { case (config, i) =>
          val clusterName = s"ccm_${i + 1}"
          val bridge = new CcmBridge(config)
          bridge.create(clusterName)
          bridge.start()
          Cluster(clusterName, config, bridge, fixture.connectionParameters)
        }
      })
    }
  }
}
