package com.datastax.driver.spark.mapper

import java.lang.reflect.Constructor

import com.datastax.driver.spark.types.TypeConverter

import scala.reflect.runtime.universe._
import scala.util.{Try, Failure, Success}

/** Factory for creating objects of any type by invoking their primary constructor and then setters.
  * Unlike Java reflection Methods or Scala reflection Mirrors, this factory is serializable
  * and can be safely passed along with Spark tasks. */
class AnyObjectFactory[T : TypeTag] extends Serializable {

  @transient
  private val tpe = implicitly[TypeTag[T]].tpe

  @transient
  private val paramTypes: Array[Type] = {
    val ctorSymbol = tpe.declaration(nme.CONSTRUCTOR).asMethod
    val ctorType = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    ctorType.params.map(_.asTerm.typeSignature).toArray
  }

  @transient
  private val rm: RuntimeMirror =
    runtimeMirror(Thread.currentThread().getContextClassLoader)

  // This must be serialized:
  val javaClass: Class[T] =
    rm.runtimeClass(tpe).asInstanceOf[Class[T]]

  // This must be also serialized:
  val argConverters: Array[TypeConverter[_]] =
    paramTypes.map(t => TypeConverter.forType(t))

  val argCount: Int = argConverters.size

  @transient
  private lazy val javaConstructor: Constructor[T] =
    javaClass.getConstructors()(0).asInstanceOf[Constructor[T]]

  // 1 if it is an inner class, 0 otherwise
  @transient
  private lazy val offset = if (javaConstructor.getParameterTypes.length > argCount) 1 else 0

  @transient
  private lazy val convertedArgs = {
    val args = Array.ofDim[AnyRef](offset + argCount)
    if (offset > 0) {
      val root = extractRoot(javaClass)
      val rootInstance = root.newInstance().asInstanceOf[AnyRef]
      args(0) = dive(rootInstance.asInstanceOf[AnyRef])
    }
    args
  }

  def newInstance(args: AnyRef*): T = {
    for (i <- 0 until argCount)
      convertedArgs(i + offset) = argConverters(i).convert(args(i)).asInstanceOf[AnyRef]

    javaConstructor.newInstance(convertedArgs: _*)
  }

  private def extractRoot(c: Class[_]): Class[_] = {
    Try {
      c.getDeclaredField("$outer")
    } match {
      case Success(outer) => extractRoot(outer.getType)
      case Failure(ex: NoSuchFieldException) => c
      case Failure(ex) => throw ex;
    }
  }

  private def dive(instance: AnyRef): AnyRef = {
    Try {
      instance.getClass.getDeclaredField("$iw")
    } match {
      case Success(iw) =>
        iw.setAccessible(true)
        dive(iw.get(instance))
      case Failure(ex: NoSuchFieldException) => instance
      case Failure(ex) => throw ex;
    }
  }

}
