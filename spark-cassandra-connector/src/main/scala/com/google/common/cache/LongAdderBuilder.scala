package com.google.common.cache

object LongAdderBuilder {

  def apply(): LongAdderWrapper = new LongAdderWrapper

  class LongAdderWrapper extends Number {
    val longAdder = new LongAdder

    def increment() = longAdder.increment()

    def add(delta: Long) = longAdder.add(delta)

    override def longValue() = longAdder.longValue()

    override def intValue(): Int = longAdder.intValue()

    override def floatValue(): Float = longAdder.floatValue()

    override def doubleValue(): Double = longAdder.doubleValue()
  }

}


