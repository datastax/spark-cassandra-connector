package com.datastax.spark.connector.writer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors}

import com.google.common.util.concurrent.MoreExecutors
import org.junit.Assert._
import org.junit.Test

class AsyncExecutorTest {

  @Test
  def test() {
    val taskCount = 20
    val maxParallel = 5

    val currentlyRunningCounter = new AtomicInteger(0)
    val maxParallelCounter = new AtomicInteger(0)
    val totalFinishedExecutionsCounter = new AtomicInteger(0)

    val task = new Callable[String] {
      override def call() = {
        val c = currentlyRunningCounter.incrementAndGet()
        var m = maxParallelCounter.get()
        while (m < c && !maxParallelCounter.compareAndSet(m, c))
          m = maxParallelCounter.get()
        Thread.sleep(100)
        currentlyRunningCounter.decrementAndGet()
        totalFinishedExecutionsCounter.incrementAndGet()
        "ok"
      }
    }

    val underlyingExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool())
    val asyncExecutor = new AsyncExecutor[Callable[String], String](underlyingExecutor.submit(_: Callable[String]), maxParallel, None, None)

    for (i <- 1 to taskCount)
      asyncExecutor.executeAsync(task)

    asyncExecutor.waitForCurrentlyExecutingTasks()
    assertEquals(maxParallel, maxParallelCounter.get())
    assertEquals(taskCount, totalFinishedExecutionsCounter.get())
    assertEquals(None, asyncExecutor.getLatestException())
  }




}
