package com.datastax.spark.connector.writer

import java.util.concurrent.Semaphore

import com.datastax.spark.connector.util.Logging
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, SettableFuture}

import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters._
import scala.util.Try
import AsyncExecutor.Handler
import com.datastax.driver.core.exceptions.{BusyPoolException, NoHostAvailableException}

/** Asynchronously executes tasks but blocks if the limit of unfinished tasks is reached. */
class AsyncExecutor[T, R](asyncAction: T => ListenableFuture[R], maxConcurrentTasks: Int,
    successHandler: Option[Handler[T]] = None, failureHandler: Option[Handler[T]]) extends Logging {

  private val semaphore = new Semaphore(maxConcurrentTasks)
  private val pendingFutures = new TrieMap[ListenableFuture[R], Boolean]
  @volatile private var latestException: Option[Throwable] = None

  /** Returns an exception if any of the futures had an exception.
    * Returning None means that no exceptions have been thrown.
    */
  def getLatestException(): Option[Throwable] = latestException

  /** Executes task asynchronously or blocks if more than `maxConcurrentTasks` limit is reached */
  def executeAsync(task: T): ListenableFuture[R] = {
    val submissionTimestamp = System.nanoTime()
    semaphore.acquire()

    val settable = SettableFuture.create[R]()
    pendingFutures.put(settable, true)

    val executionTimestamp = System.nanoTime()

    def tryFuture(): SettableFuture[R] = {
      val future = asyncAction(task)

      Futures.addCallback(future, new FutureCallback[R] {
        def release() {
          semaphore.release()
          pendingFutures.remove(settable)
        }

        def onSuccess(result: R) {
          release()
          settable.set(result)
          successHandler.foreach(_ (task, submissionTimestamp, executionTimestamp))
        }

        def onFailure(throwable: Throwable) {
          throwable match {
            case nHAE: NoHostAvailableException if nHAE.getErrors.asScala.values.exists(_.isInstanceOf[BusyPoolException]) =>
              logWarning("BusyPoolException ... Retrying")
              tryFuture()

            case otherException =>
              logError("Failed to execute: " + task, throwable)
              latestException = Some(throwable)
              release()
              settable.setException(throwable)
              failureHandler.foreach(_ (task, submissionTimestamp, executionTimestamp))
          }
        }
      })

      settable
    }

    tryFuture()
  }

  /** Waits until the tasks being currently executed get completed.     
    * It will not wait for tasks scheduled for execution during this method call,
    * nor tasks for which the [[executeAsync]] method did not complete. */
  def waitForCurrentlyExecutingTasks() {
    for ((future, _) <- pendingFutures.snapshot())
      Try(future.get())
  }
}

object AsyncExecutor {
  type Handler[T] = (T, Long, Long) => Unit
}
