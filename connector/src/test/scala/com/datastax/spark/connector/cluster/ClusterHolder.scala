package com.datastax.spark.connector.cluster

import com.datastax.spark.connector.util.{Logging, SerialShutdownHooks}
import org.apache.commons.lang3.ClassUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Source of [[TestCluster]]s for all tests executed within a single test group (single process/JVM).
  *
  * Since tests are grouped by a cluster ([[Fixture]]) and each group is executed in a separate process,
  * this cache should never have more than one element. Retrieving a different element from the one that is already
  * present in this cache results in extra time (teardown and bootstrap) wasted. Warning is logged when
  * this kind of situation is detected. */
object ClusterHolder extends Logging {

  SerialShutdownHooks.add("Connector test cluster cache shutdown hook", 100)(() => {
    close()
  })

  private val clusters: mutable.Map[String, Seq[TestCluster]] = mutable.Map()

  private def close(): Unit = synchronized {
    for (testClusters <- clusters.values; cluster <- testClusters) {
      cluster.ccmBridge.close()
    }
  }

  def get(config: Fixture): Seq[TestCluster] = {
    val key = ClassUtils.getAllInterfaces(config.getClass).asScala
      .filter(_.getPackage.equals(classOf[Fixture].getPackage))
      .map(_.getCanonicalName)
      .sorted
      .mkString("_")

    synchronized {
      clusters.getOrElseUpdate(key, {
        if (clusters.nonEmpty) {
          logWarning("Test group should contain only tests with the same cluster fixture. Verify your test group setup. " +
            s"Stopping previous clusters and bootstrapping the new one for ${config.getClass.getCanonicalName}")
          for (testClusters <- clusters.values; cluster <- testClusters) {
            cluster.ccmBridge.close()
          }
          clusters.clear()
        }

        config.builders.zipWithIndex.map { case (builder, i) =>

          val clusterName = s"ccm_${i + 1}"
          val bridge = builder.build()
          bridge.create(clusterName)
          bridge.start()
          TestCluster(clusterName, bridge)
        }
      })
    }
  }
}
