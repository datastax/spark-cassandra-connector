package com.datastax.spark.connector {

case class SampleScalaCaseClass(key: Int, value: String)

class SampleScalaClass(val key: Int, val value: String)

class SampleScalaClassWithNoFields(key: Int, value: String)

class SampleScalaClassWithMultipleCtors(val key: Int, val value: String) {
  def this(key: Int) = this(key, null)

  def this() = this(0, null)
}

class SampleWithNestedScalaCaseClass {

  case class SampleNestedScalaCaseClass(key: Int, value: String)

}

}