package com.datastax.spark.connector.writer

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, SimpleStatement, Statement}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, CompletableFuture, CompletionStage}

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Matchers._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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

    def execute(callable: Callable[String]): CompletionStage[String] = {
      import ExecutionContext.Implicits.global
      val completableFuture = new CompletableFuture[String]()
      Future { callable.call() }.onComplete {
        case Success(str) => completableFuture.complete(str)
        case Failure(exception) => completableFuture.completeExceptionally(exception)
      }
      completableFuture
    }

    val asyncExecutor = new AsyncExecutor[Callable[String], String](execute, maxParallel, None, None)

    for (i <- 1 to taskCount)
      asyncExecutor.executeAsync(task)

    asyncExecutor.waitForCurrentlyExecutingTasks()

    maxParallelCounter.get() should be <= maxParallel
    totalFinishedExecutionsCounter.get() shouldBe taskCount
    asyncExecutor.getLatestException() shouldBe None
  }

  @Test
  def testGracefullyHandleCqlSessionExecuteExceptions() {
    val executor = new AsyncExecutor[Statement[_], AsyncResultSet](
      _ => {
        // simulate exception returned by session.executeAsync() (not future)
        throw new IllegalStateException("something bad happened")
      }, 10, None, None
    )
    val stmt = SimpleStatement.newInstance("INSERT INTO table1 (key, value) VALUES (1, '100')");
    val future = executor.executeAsync(stmt)
    assertTrue(future.isCompleted)
    val value = future.value.get
    assertTrue(value.isInstanceOf[Failure[_]])
    assertTrue(value.asInstanceOf[Failure[_]].exception.isInstanceOf[IllegalStateException])
  }
}
