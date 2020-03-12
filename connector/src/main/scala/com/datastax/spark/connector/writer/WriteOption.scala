package com.datastax.spark.connector.writer

import java.util.Date

import scala.concurrent.duration.{Duration ⇒ ScalaDuration}

import org.joda.time.{DateTime, Duration ⇒ JodaDuration}

sealed trait WriteOptionValue[+T]

case class StaticWriteOptionValue[T](value: T) extends WriteOptionValue[T]

case class PerRowWriteOptionValue[T](placeholder: String) extends WriteOptionValue[T]

sealed trait WriteOption[+T]

case class TTLOption(value: WriteOptionValue[Int]) extends WriteOption[Int]

case class TimestampOption(value: WriteOptionValue[Long]) extends WriteOption[Long]

case object DefaultValue extends WriteOptionValue[Nothing]

object WriteOption {
  def unapply(writeOption: WriteOption[_]): Option[WriteOptionValue[_]] = writeOption match {
    case TTLOption(value) => Some(value)
    case TimestampOption(value) => Some(value)
    case _ => None
  }
}

object TTLOption {

  val defaultValue = TTLOption(DefaultValue)

  def forever: TTLOption = TTLOption(StaticWriteOptionValue[Int](0))

  /** @param ttl TTL in seconds */
  def constant(ttl: Int): TTLOption = {
    require(ttl > 0, "Explicitly specified TTL must be greater than zero.")
    TTLOption(StaticWriteOptionValue(ttl))
  }

  def constant(ttl: JodaDuration): TTLOption = constant(ttl.getStandardSeconds.toInt)

  def constant(ttl: ScalaDuration): TTLOption = if (ttl.isFinite()) constant(ttl.toSeconds.toInt) else forever

  def perRow(placeholder: String): TTLOption = TTLOption(PerRowWriteOptionValue[Int](placeholder))

}

object TimestampOption {

  val defaultValue = TimestampOption(DefaultValue)

  def constant(microseconds: Long): TimestampOption = {
    require(microseconds > 0, "Explicitly specified time must be greater than zero.")
    TimestampOption(StaticWriteOptionValue(microseconds))
  }

  def constant(timestamp: Date): TimestampOption = constant(timestamp.getTime * 1000L)

  def constant(timestamp: DateTime): TimestampOption = constant(timestamp.getMillis * 1000L)

  def perRow(placeholder: String): TimestampOption =
    TimestampOption(PerRowWriteOptionValue(placeholder))
}
