package com.datastax.spark.connector {

case class SampleScalaCaseClass(key: Int, value: String)

class SampleScalaClass(val key: Int, val value: String)

class SampleScalaClassWithNoFields(key: Int, value: String)

class SampleScalaClassWithMultipleCtors(val key: Int, val value: String) {
  def this(key: Int) = this(key, null)

  def this() = this(0, null)
}

class SampleWithNestedScalaCaseClass {

  case class InnerClass(key: Int, value: String)

}

class SampleWithDeeplyNestedScalaCaseClass {

  class IntermediateClass {

    case class InnerClass(key: Int, value: String)

  }

}

object SampleObject {

  case class ClassInObject(key: Int, value: String)

}

}