package com.datastax.spark.connector.util

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext

object Threads {

  implicit val BlockingIOExecutionContext = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("spark-cassandra-connector-io" + "%d")
      .build
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool(threadFactory))
  }
}

