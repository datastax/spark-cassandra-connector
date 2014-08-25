package com.datastax.spark.connector.util

import scala.collection.concurrent.TrieMap
import scala.reflect.runtime.universe._
import scala.util.{Try, Success, Failure}

object ReflectionUtil {
  private val rm = runtimeMirror(getClass.getClassLoader)
  private val singletonCache = TrieMap[String, Any]()

  private def findScalaObject[T : TypeTag](objectName: String): Try[T] = {
    Try {
      val targetType = implicitly[TypeTag[T]].tpe
      val module = rm.staticModule(objectName)
      if (!(module.typeSignature <:< targetType))
        throw new IllegalArgumentException(s"Object $objectName is not instance of $targetType")

      val moduleMirror = rm.reflectModule(module)
      moduleMirror.instance.asInstanceOf[T]
    }
  }

  private def findSingletonClassInstance[T : TypeTag](className: String): Try[T] = {
    Try {
      val targetType = implicitly[TypeTag[T]].tpe
      val targetClass = rm.runtimeClass(targetType.typeSymbol.asClass)
      val instance =
        singletonCache.get(className) match {
          case Some(obj) => obj
          case None =>
            val newInstance = Class.forName(className).getConstructor(Array.empty[Class[_]]: _*).newInstance()
            singletonCache.putIfAbsent(className, newInstance) match {
              case None => newInstance
              case Some(previousInstance) => previousInstance
            }
        }

      if (!targetClass.isInstance(instance))
        throw new IllegalArgumentException(s"Class $className is not $targetType")
      instance.asInstanceOf[T]
    }
  }

  /** Returns either a global Scala object by its fully qualified name or a singleton
    * instance of a Java class identified by its fully qualified class name.
    * Java class instances are cached. The Java class must provide a default constructor. */
  def findGlobalObject[T : TypeTag](objectName: String): T = {
    val scalaObject: Try[T] = findScalaObject[T](objectName)
    val classInstance: Try[T] = findSingletonClassInstance[T](objectName)
    scalaObject orElse classInstance match {
      case Success(obj) => obj
      case Failure(e) => throw new IllegalArgumentException(s"Singleton object not available: $objectName", e)
    }
  }
}
