package com.datastax.driver.spark.connector

import java.util.concurrent.{ThreadFactory, TimeUnit, Executors}

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap


/** A lockless cache that caches values for multiple users
  * and destroys them once all users release them. One value can be associated with many keys.
  * Useful for sharing a costly resource.
  * @param create function to create new objects if not found in cache
  * @param destroy function to be called once the value is not used any more
  * @param keys function generating additional keys the value should be reachable by
  * @param releaseDelayMillis number of milliseconds to keep unused values in cache, before they are removed. */
final class RefCountedCache[K, V](create: K => V,
                                  destroy: V => Any,
                                  keys: (K, V) => Set[K] = (_: K, _: V) => Set.empty[K],
                                  releaseDelayMillis: Int = 0) {

  if (releaseDelayMillis < 0)
    throw new IllegalArgumentException(s"releaseDelayMillis can't be negative: $releaseDelayMillis")

  private case class ReleaseTask(value: V, count: Int, scheduledTime: Long) extends Runnable {
    def run() {
      releaseImmediately(value, count)
    }
  }

  private val refCounter = new RefCountMap[V]
  private val cache = new TrieMap[K, V]
  private val valuesToKeys = new TrieMap[V, Set[K]]
  private val deferredReleases = new TrieMap[V, ReleaseTask]

  private def createNewValueAndKeys(key: K): (V, Set[K]) = {
    val value = create(key)
    try {
      (value, keys(key, value) + key)
    }
    catch {
      case t: Throwable =>
        destroy(value)
        throw t
    }
  }

  /** Acquires a value associated with key.
    * If the value was acquired by another thread and is present in the cache, it will be returned from cache.
    * If the value was not found in cache, a new value will be created by invoking `create` function
    * and will be saved to the cache and associated with `key` and other keys returned by invoking the
    * `keys` function on the value. */
  @tailrec
  def acquire(key: K): V = {
    cache.get(key) match {
      case Some(value) =>
        if (refCounter.acquireIfNonZero(value) > 0)
          value
        else
          acquire(key)
      case None =>
        val (value, keySet) = createNewValueAndKeys(key)
        refCounter.acquire(value)
        cache.putIfAbsent(key, value) match {
          case None =>
            keySet.foreach(k => cache.put(k, value))
            valuesToKeys.put(value, keySet)
            value
          case Some(otherValue) =>
            destroy(value)
            refCounter.release(value)
            if (refCounter.acquireIfNonZero(otherValue) > 0)
              otherValue
            else
              acquire(key)
        }
    }
  }

  private def releaseImmediately(value: V, count: Int = 1) {
    if (refCounter.release(value, count) == 0) {
      // Here we're sure no-one else has the value.
      // Even though it is still in cache, it is not possible to get it from there,
      // because it won't be possible to increase the reference counter from 0.
      valuesToKeys(value).foreach(k => cache.remove(k, value))
      valuesToKeys.remove(value)
      destroy(value)
    }
  }

  @tailrec
  private def releaseDeferred(value: V, count: Int = 1) {
    val newTime = System.currentTimeMillis() + releaseDelayMillis
    val newTask =
      deferredReleases.remove(value) match {
        case Some(oldTask) =>
          ReleaseTask(value, oldTask.count + count, newTime)
        case None =>
          ReleaseTask(value, count, newTime)
      }
    deferredReleases.putIfAbsent(value, newTask) match {
      case Some(oldTask) =>
        releaseDeferred(value, newTask.count)
      case None =>
    }
  }

  /** Releases previously acquired value. Once the value is released by all threads and
    * the `releaseDelayMillis` timeout passes, the value is destroyed by calling `destroy` function and
    * removed from the cache. */
  def release(value: V) {
    if (releaseDelayMillis == 0 || scheduledExecutorService.isShutdown)
      releaseImmediately(value)
    else
      releaseDeferred(value)
  }

  /** Shuts down the background deferred `release` scheduler and forces all pending release tasks to be executed */
  def shutdown() {
    scheduledExecutorService.shutdown()
    while (deferredReleases.nonEmpty)
      for ((value, task) <- deferredReleases.snapshot())
        if (deferredReleases.remove(value, task))
          task.run()
  }

  /** Removes all entries from the cache and destroys stored values by calling `destroy` on them.
    * Warning - this is not thread-safe. You must not call this method if you know
    * the cache is in use.*/
  def evict() {
    for ((key, value) <- cache)
      destroy(value)
    deferredReleases.clear()
    cache.clear()
    valuesToKeys.clear()
    refCounter.clear()
  }

  /** Returns true if cache contains given key. */
  def contains(key: K): Boolean = {
    val value = cache.get(key)
    value.isDefined && refCounter.get(value.get) > 0
  }

  /** Called periodically by `scheduledExecutorService`*/
  private def processPendingReleases() {
    val now = System.currentTimeMillis()
    for ((value, task) <- deferredReleases)
      if (task.scheduledTime <= now)
        if (deferredReleases.remove(value, task))
          task.run()
        // if the task has been replaced in the meantime and remove fails, that's fine, because
        // for sure it has scheduled time set in the future; we'll get to it on the next processPendingReleases call
  }

  private val processPendingReleasesTask = new Runnable() {
    override def run() {
      processPendingReleases()
    }
  }

  private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    override def newThread(r: Runnable) = {
      val thread = Executors.defaultThreadFactory().newThread(r)
      thread.setDaemon(true)
      thread
    }
  })

  if (releaseDelayMillis > 0) {
    val period = math.max(10, releaseDelayMillis)
    scheduledExecutorService.scheduleAtFixedRate(processPendingReleasesTask, period, period, TimeUnit.MILLISECONDS)
  }

}
