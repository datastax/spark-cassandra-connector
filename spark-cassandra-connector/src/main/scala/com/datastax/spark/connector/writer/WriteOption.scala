package com.datastax.spark.connector.writer

import java.util.Date

import org.apache.spark.streaming.{Duration => SparkDuration}
import org.joda.time.{DateTime, Duration => JodaDuration}

import scala.concurrent.duration.{Duration => ScalaDuration}

sealed trait WriteOption[+T]

case class StaticWriteOption[T](value: T) extends WriteOption[T]

case class PerRowWriteOption[T](placeholder: String) extends WriteOption[T]

case object AutoWriteOption extends WriteOption[Nothing]

object TTLOption {
  def auto: WriteOption[Int] = AutoWriteOption
         
  def forever = StaticWriteOption[Int](0)

  def constant(ttl: Int): StaticWriteOption[Int] = {
    require(ttl > 0, "Explicitly specified TTL must be greater than zero.")
    StaticWriteOption[Int](ttl)
  }

  def constant(ttl: SparkDuration): StaticWriteOption[Int] = constant((ttl.milliseconds / 1000L).toInt)

  def constant(ttl: JodaDuration): StaticWriteOption[Int] = constant(ttl.getStandardSeconds.toInt)

  def constant(ttl: ScalaDuration): StaticWriteOption[Int] = if (ttl.isFinite()) constant(ttl.toSeconds.toInt) else forever

  def perRow(placeholder: String): PerRowWriteOption[Int] =
    PerRowWriteOption[Int](placeholder)

}

object TimestampOption {
  def auto: WriteOption[Long] = AutoWriteOption

  def constant(microseconds: Long): StaticWriteOption[Long] = {
    require(microseconds > 0, "Explicitly specified time must be greater than zero.")
    StaticWriteOption[Long](microseconds)
  }

  def constant(timestamp: Date): StaticWriteOption[Long] = constant(timestamp.getTime * 1000L)

  def constant(timestamp: DateTime): StaticWriteOption[Long] = constant(timestamp.getMillis * 1000L)

  def perRow(placeholder: String): PerRowWriteOption[Long] =
    PerRowWriteOption[Long](placeholder)
}