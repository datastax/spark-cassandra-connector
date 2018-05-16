package com.datastax.spark.connector.util

import scala.collection.mutable

private[connector] object SerialShutdownHooks extends Logging {

  private case class PriorityShutdownHook(
      name: String,
      priority: Int,
      task: () => Unit
  )

  private val hooks = mutable.ListBuffer[PriorityShutdownHook]()
  private var isShuttingDown = false

  /** Adds given hook with given priority. The higher the priority, the sooner the hook is executed. */
  def add(name: String, priority: Int)(task: () => Unit): Unit = SerialShutdownHooks.synchronized {
    if (isShuttingDown) {
      logError(s"Adding shutdown hook ($name) during shutting down is not allowed.")
    } else {
      hooks.append(PriorityShutdownHook(name, priority, task))
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread("Serial shutdown hooks thread") {
    override def run(): Unit = {
      SerialShutdownHooks.synchronized {
        isShuttingDown = true
      }
      val prioritizedHooks = hooks.sortBy(-_.priority)
      for (hook <- prioritizedHooks) {
        try {
          logDebug(s"Running shutdown hook: ${hook.name}")
          hook.task()
          logInfo(s"Successfully executed shutdown hook: ${hook.name}")
        } catch {
          case exc: Throwable =>
            logError(s"Shutdown hook (${hook.name}) failed", exc)
        }
      }
    }
  })
}
