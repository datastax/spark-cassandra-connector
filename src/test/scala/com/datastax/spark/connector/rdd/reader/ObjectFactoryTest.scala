package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector.util.JavaApiHelper
import com.datastax.spark.connector._
import org.scalatest.{Matchers, WordSpec}
import scala.reflect.runtime.universe._

class ObjectFactoryTest extends WordSpec with Matchers {

  "JavaObjectFactory" when {
    "instantiated for a bean class with a single, no-args constructor" should {

      val factory = new JavaObjectFactory[SampleJavaBean]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance()
        instance shouldBe a[SampleJavaBean]
      }

      "return 0 with argCount" in {
        factory.argCount should be(0)
      }

      "return empty collection with constructorParamTypes" in {
        factory.constructorParamTypes should have size 0
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleJavaBean])
      }
    }

    "instantiated for a bean class with multiple constructors which include no-args constructor" should {

      val factory = new JavaObjectFactory[SampleJavaBeanWithMultipleCtors]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance()
        instance shouldBe a[SampleJavaBeanWithMultipleCtors]
      }

      "return 0 with argCount" in {
        factory.argCount should be(0)
      }

      "return empty collection with constructorParamTypes" in {
        factory.constructorParamTypes should have size 0
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleJavaBeanWithMultipleCtors])
      }
    }

    "tried to be instantiated for an unsupported bean class" should {

      "throw NoSuchMethodException if class does not have no-args constructor" in {
        intercept[NoSuchMethodException] {
          val factory = new JavaObjectFactory[SampleJavaBeanWithoutNoArgsCtor]
        }
      }

      "throw IllegalArgumentException if class is a member class" in {
        implicit val tt = JavaApiHelper.getTypeTag(classOf[SampleWithNestedJavaBean#SampleNestedJavaBean])
        intercept[IllegalArgumentException] {
          val factory = new JavaObjectFactory[SampleWithNestedJavaBean#SampleNestedJavaBean]
        }
      }

    }
  }

  "AnyObjectFactory" when {
    "instantiated for a Scala case class" should {
      val factory = new AnyObjectFactory[SampleScalaCaseClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaCaseClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaCaseClass])
      }
    }

    "instantiated for a Scala class" should {
      val factory = new AnyObjectFactory[SampleScalaClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaClass])
      }
    }

    "instantiated for a Scala class without fields" should {
      val factory = new AnyObjectFactory[SampleScalaClassWithNoFields]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClassWithNoFields]
      }

      "return 2 with argCount" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaClassWithNoFields])
      }
    }

    "instantiated for a nested Scala class" should {
      val factory = new AnyObjectFactory[SampleWithNestedScalaCaseClass#SampleNestedScalaCaseClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleWithNestedScalaCaseClass#SampleNestedScalaCaseClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithNestedScalaCaseClass#SampleNestedScalaCaseClass])
      }
    }

    "tried to be instantiated for an unsupported class" should {

      "throw ScalaReflectionException if class has mutiple constructors" in {
        intercept[ScalaReflectionException] {
          val factory = new AnyObjectFactory[SampleScalaClassWithMultipleCtors]
        }
      }
    }
  }

}
