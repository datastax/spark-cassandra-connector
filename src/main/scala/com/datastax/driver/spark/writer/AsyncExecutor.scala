package com.datastax.driver.spark.writer

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, SettableFuture}

import scala.collection.concurrent.TrieMap
import scala.util.Try

/** Asynchronously executes tasks but blocks if the limit of unfinished tasks is reached. */
class AsyncExecutor[T, R](asyncAction: T => ListenableFuture[R], maxConcurrentTasks: Int) {

  private val _successCount = new AtomicInteger(0)
  private val _failureCount = new AtomicInteger(0)

  private val semaphore = new Semaphore(maxConcurrentTasks)
  private val pendingFutures = new TrieMap[ListenableFuture[R], Boolean]

  /** Executes task asynchronously or blocks if more than `maxConcurrentTasks` limit is reached */
  def executeAsync(task: T): ListenableFuture[R] = {
    semaphore.acquire()

    val settable = SettableFuture.create[R]()
    Futures.addCallback(settable, new FutureCallback[R] {
      override def onSuccess(result: R): Unit = pendingFutures.remove(settable)
      override def onFailure(t: Throwable): Unit = pendingFutures.remove(settable)
    })
    pendingFutures.put(settable, true)

    val future = asyncAction(task)

    Futures.addCallback(future, new FutureCallback[R] {
      def onSuccess(result: R) {
        _successCount.incrementAndGet()
        semaphore.release()
        settable.set(result)
      }
      def onFailure(throwable: Throwable) {
        _failureCount.incrementAndGet()
        semaphore.release()
        settable.setException(throwable)
      }
    })

    settable
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
