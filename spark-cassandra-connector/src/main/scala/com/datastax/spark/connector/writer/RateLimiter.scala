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
  *
  * @param rate maximum allowed long-term rate per 1000 units of time
  * @param bucketSize maximum acceptable "burst"
  * @param time source of time; typically 1 unit = 1 ms
  * @param sleep a function to call to slow down the calling thread;
  *              must use the same time units as `time`
  */
class RateLimiter(
    rate: Long,
    bucketSize: Long,
    time: () => Long = System.currentTimeMillis,
    sleep: Long => Any = Thread.sleep) {

  private val bucketFill = new AtomicLong(0L)
  private val lastTime = new AtomicLong(time())

  @tailrec
  private def leak(toLeak: Long): Unit = {
    val fill = bucketFill.get()
    val reallyToLeak = math.min(fill, toLeak)  // we can't leak more than there is now
    if (!bucketFill.compareAndSet(fill, fill - reallyToLeak))
      leak(reallyToLeak)
  }

  private def leak(): Unit = {
    val currentTime = time()
    val prevTime = lastTime.getAndSet(currentTime)
    val elapsedTime = math.max(currentTime - prevTime, 0) // Protect against funky clocks
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
      sleep(delay)
  }
}
