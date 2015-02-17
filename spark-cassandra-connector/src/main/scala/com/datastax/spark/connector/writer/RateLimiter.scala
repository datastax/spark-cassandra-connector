package com.datastax.spark.connector.writer

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec

/** A leaking bucket rate limiter.
  * It can be used to limit rate of anything,
  * but typically it is used to limit rate of data transfer.
  *
  * It starts with an empty bucket.
  * When packets arrive, they are added to the bucket.
  * The bucket has a constant size and is leaking at a constant rate.
  * If the bucket overflows, the thread is delayed by the
  * amount of time proportional to the amount of the overflow.
  *
  * This class is thread safe and lockless.
  */
class RateLimiter(rate: Long, bucketSize: Long) {

  private val bucketFill = new AtomicLong(0L)
  private val lastTime = new AtomicLong(0L)

  @tailrec
  private def leak(toLeak: Long): Unit = {
    val fill = bucketFill.get()
    val reallyToLeak = math.min(fill, toLeak)  // we can't leak more than there is now
    if (!bucketFill.compareAndSet(fill, fill - reallyToLeak))
      leak(toLeak)
  }

  private def leak(): Unit = {
    val currentTime = System.currentTimeMillis()
    val prevTime = lastTime.getAndSet(currentTime)
    val elapsedTime = currentTime - prevTime
    leak(elapsedTime * rate / 1000)
  }

  /** Processes a single packet.
    * If the packet is bigger than the current amount of
    * space available in the bucket, this method will
    * sleep for appropriate amount of time, in order
    * to not exceed the target rate. */
  def maybeSleep(packetSize: Long): Unit = {
    leak()
    val currentFill = bucketFill.addAndGet(packetSize)
    val overflow = currentFill - bucketSize
    val delay = 1000 * overflow / rate
    if (delay > 0)
      Thread.sleep(delay)
  }
}
