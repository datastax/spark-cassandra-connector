package com.datastax.spark.connector.util

import scala.collection.mutable

private[connector] object SerialShutdownHooks extends Logging {

  private val hooks = mutable.Map[String, () => Unit]()
  @volatile private var isShuttingDown = false

  def add(name: String)(body: () => Unit): Unit = SerialShutdownHooks.synchronized {
    if (isShuttingDown) {
      logError(s"Adding shutdown hook ($name) during shutting down is not allowed.")
    } else {
      hooks.put(name, body)
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread("Serial shutdown hooks thread") {
    override def run(): Unit = {
      SerialShutdownHooks.synchronized {
        isShuttingDown = true
      }
      for ((name, task) <- hooks) {
        try {
          logDebug(s"Running shutdown hook: $name")
          task()
          logInfo(s"Successfully executed shutdown hook: $name")
        } catch {
          case exc: Throwable =>
            logError(s"Shutdown hook ($name) failed", exc)
        }
      }
    }
  })
}
