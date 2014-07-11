package com.datastax.spark.connector.writer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Future, Semaphore}

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.collection.concurrent.TrieMap
import scala.util.Try


/** Asynchronously executes tasks but blocks if the limit of unfinished tasks is reached. */
class AsyncExecutor[T, R](asyncAction: T => ListenableFuture[R], maxConcurrentTasks: Int) {

  private val _successCount = new AtomicInteger(0)
  private val _failureCount = new AtomicInteger(0)

  private val semaphore = new Semaphore(maxConcurrentTasks)
  private val pendingFutures = new TrieMap[Future[R], Boolean]

  /** Executes task asynchronously or blocks if more than `maxConcurrentTasks` limit is reached */
  def executeAsync(task: T): ListenableFuture[R] = {
    semaphore.acquire()
    val future = asyncAction(task)
    pendingFutures.put(future, true)

    Futures.addCallback(future, new FutureCallback[R] {
      def release() {
        semaphore.release()
        pendingFutures.remove(future)
      }
      def onSuccess(p1: R) { _successCount.incrementAndGet(); release() }
      def onFailure(p1: Throwable) { _failureCount.incrementAndGet(); release() }
    })

    future
  }

  /** Waits until the tasks being currently executed get completed.     
    * It will not wait for tasks scheduled for execution during this method call,
    * nor tasks for which the [[executeAsync]] method did not complete. */
  def waitForCurrentlyExecutingTasks() {
    for ((future, _) <- pendingFutures.snapshot())
      Try(future.get())
  }

  def successCount = _successCount.get()
  def failureCount = _failureCount.get()

}
