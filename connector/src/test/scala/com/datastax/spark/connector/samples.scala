package com.datastax.spark.connector {

case class SampleScalaCaseClass(key: Int, value: String)

class SampleScalaClass(val key: Int, val value: String) extends Serializable

class SampleScalaClassWithNoFields(key: Int, value: String) extends Serializable

class SampleScalaClassWithMultipleCtors(var key: Int, var value: String) extends Serializable {
  def this(key: Int) = this(key, null)

  def this() = this(0, null)
}

class SampleWithNestedScalaCaseClass extends Serializable {

  case class InnerClass(key: Int, value: String)

}

class SampleWithDeeplyNestedScalaCaseClass extends Serializable {

  class IntermediateClass extends Serializable {

    case class InnerClass(key: Int, value: String)

  }

}

object SampleObject {

  case class ClassInObject(key: Int, value: String)

}

}