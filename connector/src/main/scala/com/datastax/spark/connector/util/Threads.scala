package com.datastax.spark.connector.util

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Threads extends Logging {

  implicit val BlockingIOExecutionContext: ExecutionContextExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("spark-cassandra-connector-io" + "%d")
      .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logWarning(s"Unhandled exception in thread ${t.getName}.", e)
        }
      })
      .build
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool(threadFactory))
  }
}

