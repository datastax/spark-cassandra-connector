package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector._
import com.datastax.spark.connector.util.JavaApiHelper
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.runtime.universe._

class AnyObjectFactoryTest extends WordSpec with Matchers {

  "AnyObjectFactory" when {
    "instantiated for a bean class with a single, no-args constructor" should {

      val factory = new AnyObjectFactory[SampleJavaBean]

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
      val factory = new AnyObjectFactory[SampleJavaBeanWithMultipleCtors]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance()
        instance shouldBe a[SampleJavaBeanWithMultipleCtors]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleJavaBeanWithMultipleCtors])
      }
    }

    "instantiated for an inner Java class" should {
      implicit val tt = JavaApiHelper.getTypeTag(classOf[SampleWithNestedJavaBean#InnerClass])
      val factory = new AnyObjectFactory[SampleWithNestedJavaBean#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance()
        instance shouldBe a[SampleWithNestedJavaBean#InnerClass]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithNestedJavaBean#InnerClass])
      }
    }

    "instantiated for a deeply nested inner Java class" should {
      implicit val tt = JavaApiHelper.getTypeTag(classOf[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass])
      val factory = new AnyObjectFactory[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithDeeplyNestedJavaBean#IntermediateClass#InnerClass])
      }
    }

    "tried to be instantiated for an unsupported bean class" should {

      "throw NoSuchMethodException if class does not have suitable constructor" in {
        intercept[NoSuchMethodException] {
          new AnyObjectFactory[SampleJavaBeanWithoutNoArgsCtor]
        }
      }
    }

    "instantiated for a Scala case class with 2 args constructor" should {
      val factory = new AnyObjectFactory[SampleScalaCaseClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaCaseClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount because the only constructor of this case class has two args" in {
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

    "instantiated for a Scala case class with 2 args constructor which is defined inside an object" should {
      val factory = new AnyObjectFactory[SampleObject.ClassInObject]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleObject.ClassInObject]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount because the only constructor of this case class has two args" in {
        factory.argCount should be(2)
      }

      "return collection of {Int, String} types with constructorParamTypes" in {
        factory.constructorParamTypes.zip(Array(typeOf[Int], typeOf[String])).foreach {
          case (t1, t2) => (t1 =:= t2) should be(true)
        }
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleObject.ClassInObject])
      }
    }

    "instantiated for a Scala class with 2 args constructor" should {
      val factory = new AnyObjectFactory[SampleScalaClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClass]
        instance.key should be(1)
        instance.value should be("one")
      }

      "return 2 with argCount because the only constructor of this class has 2 args" in {
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

    "instantiated for a Scala class with 2 args constructor and without fields" should {
      val factory = new AnyObjectFactory[SampleScalaClassWithNoFields]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClassWithNoFields]
      }

      "return 2 with argCount because the only constructor of this class has 2 args" in {
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

    "instantiated for a Scala class with multiple constructors" should {
      val factory = new AnyObjectFactory[SampleScalaClassWithMultipleCtors]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleScalaClassWithMultipleCtors]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleScalaClassWithMultipleCtors])
      }
    }

    "instantiated for an inner Scala class with 2 args constructor" should {
      val factory = new AnyObjectFactory[SampleWithNestedScalaCaseClass#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleWithNestedScalaCaseClass#InnerClass]
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
        factory.javaClass should be(classOf[SampleWithNestedScalaCaseClass#InnerClass])
      }
    }

    "instantiated for a deeply nested inner Scala class" should {
      val factory = new AnyObjectFactory[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass]

      "create an instance of that class with newInstance" in {
        val instance = factory.newInstance(1.asInstanceOf[AnyRef], "one".asInstanceOf[AnyRef])
        instance shouldBe a[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass]
      }

      "return that class with javaClass" in {
        factory.javaClass should be(classOf[SampleWithDeeplyNestedScalaCaseClass#IntermediateClass#InnerClass])
      }
    }

  }

}
