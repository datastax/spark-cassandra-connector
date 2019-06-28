package com.datastax.spark.connector.util

import scala.reflect.runtime.universe._
import org.scalatest.concurrent.Conductors

import com.datastax.spark.connector.cql.{CassandraConnectorConf, DefaultConnectionFactory, CassandraConnectionFactory}
import org.scalatest.{FlatSpec, Matchers}

trait ReflectionUtilSpecDummyTrait
object ReflectionUtilSpecDummyObject extends ReflectionUtilSpecDummyTrait

class ReflectionUtilSpec extends FlatSpec with Matchers with Conductors {

  "ReflectionUtil.findGlobalObject" should "be able to find DefaultConnectionFactory" in {
    val factory = ReflectionUtil.findGlobalObject[CassandraConnectionFactory](
      "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    factory should be(DefaultConnectionFactory)
  }

  it should "be able to find a global object in a multi-threaded context" in {
    val conductor = new Conductor
    import conductor._

    thread("ThreadA") {
      val obj = ReflectionUtil.findGlobalObject[ReflectionUtilSpecDummyTrait](
        "com.datastax.spark.connector.util.ReflectionUtilSpecDummyObject")
      obj should be(ReflectionUtilSpecDummyObject)
    }
    thread("ThreadB") {
      val obj = ReflectionUtil.findGlobalObject[ReflectionUtilSpecDummyTrait](
        "com.datastax.spark.connector.util.ReflectionUtilSpecDummyObject")
      obj should be(ReflectionUtilSpecDummyObject)

    }
    conduct()
  }

  it should "be able to instantiate a singleton object based on Java class name" in {
    val obj = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    obj should be ("")
  }

  it should "cache Java class instances" in {
    val obj1 = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    val obj2 = ReflectionUtil.findGlobalObject[String]("java.lang.String")
    obj1 shouldBe theSameInstanceAs (obj2)
  }

  it should "throw IllegalArgumentException when asked for a Scala object of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[CassandraConnectorConf](
        "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    }
  }

  it should "throw IllegalArgumentException when asked for class instance of wrong type" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[Integer]("java.lang.String")
    }
  }

  it should "throw IllegalArgumentException when object does not exist" in {
    intercept[IllegalArgumentException] {
      ReflectionUtil.findGlobalObject[CassandraConnectorConf]("NoSuchObject")
    }
  }

  private class ClassWithConstructor(arg1: Int, arg2: List[String])

  "ReflectionUtil.constructorParams" should "return proper constructor param names and types for a class with a single constructor" in {
    val params = ReflectionUtil.constructorParams[ClassWithConstructor]
    params should have size 2
    params(0)._1 should be("arg1")
    params(1)._1 should be("arg2")
    assert(params(0)._2 =:= typeOf[Int])
    assert(params(1)._2 =:= typeOf[List[String]])
  }

  private class ClassWithTwoConstructors(arg1: Int, arg2: List[String]) {
    def this(arg1: Int) = this(arg1, List.empty)
  }

  it should "return main constructor's param names and types for a class with multiple constructors" in {
    val params = ReflectionUtil.constructorParams[ClassWithTwoConstructors]
    params should have size 2
    params(0)._1 should be("arg1")
    params(1)._1 should be("arg2")
    assert(params(0)._2 =:= typeOf[Int])
    assert(params(1)._2 =:= typeOf[List[String]])
  }

  private class ClassWithGetters(val arg1: Int, val arg2: List[String]) {
    def getterLikeMethod: String = null

    def otherMethod(x: String): String = null
  }

  "ReflectionUtil.getters" should "return getter names and types" in {
    val getters = ReflectionUtil.getters[ClassWithGetters].toMap
    getters should have size 2
    getters.keySet should contain ("arg1")
    getters.keySet should contain ("arg2")
    assert(getters("arg1") =:= typeOf[Int])
    assert(getters("arg2") =:= typeOf[List[String]])
  }

  private class ClassWithSetters(var arg1: Int, var arg2: List[String]) {
    def setterLikeMethod_=(x: String): Unit = {}

    def otherMethod(x: String): Unit = {}
  }

  "ReflectionUtil.setters" should "return setter names and types" in {
    val setters = ReflectionUtil.setters[ClassWithSetters].toMap
    setters should have size 2
    setters.keySet should contain ("arg1_$eq")
    setters.keySet should contain ("arg2_$eq")
    assert(setters("arg1_$eq") =:= typeOf[Int])
    assert(setters("arg2_$eq") =:= typeOf[List[String]])
  }

  "ReflectionUtil.methodParamTypes" should "return method param types" in {
    val paramTypes = ReflectionUtil.methodParamTypes(typeOf[ClassWithGetters], "otherMethod")
    paramTypes should have size 1
    assert(paramTypes.head =:= typeOf[String])
  }

  private class GenericClass[T] {
    def genericMethod(param: T): Unit = {}
  }

  it should "return proper method param types for generic type" in {
    val paramTypes = ReflectionUtil.methodParamTypes(typeOf[GenericClass[Int]], "genericMethod")
    paramTypes should have size 1
    assert(paramTypes.head =:= typeOf[Int], "because returned param type should be Int")
  }

  it should "throw IllegalArgumentException if the requested method is missing" in {
    val exception = the [IllegalArgumentException] thrownBy
      ReflectionUtil.methodParamTypes(typeOf[ClassWithGetters], "unknownMethodName")
    exception.getMessage should include ("unknownMethodName")
    exception.getMessage should include ("ClassWithGetters")
  }

}
