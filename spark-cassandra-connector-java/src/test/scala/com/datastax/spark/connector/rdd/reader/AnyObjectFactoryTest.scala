package com.datastax.spark.connector.rdd.reader

import com.datastax.spark.connector._
import com.datastax.spark.connector.util.JavaApiHelper

class AnyObjectFactoryTest extends AbstractSpec {

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
        val instance = newInstance(factory)
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
        val instance = newInstance(factory)
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
        val instance = newInstance(factory)
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
  }

  private def newInstance[T](factory: AnyObjectFactory[T]): T = factory.argCount match {
    case 0 ⇒ factory.newInstance()
    case 1 ⇒ factory.newInstance(1.asInstanceOf[AnyRef])
    case 2 ⇒ factory.newInstance(1.asInstanceOf[AnyRef], "one")
  }

}
