package com.datastax.spark.connector.util

import scala.reflect.runtime.universe._

object ReflectionUtil {
  val rm = runtimeMirror(getClass.getClassLoader)

  def findGlobalObject[T : TypeTag](objectName: String): T = {
    try {
      val targetType = implicitly[TypeTag[T]].tpe
      val module = rm.staticModule(objectName)
      if (!(module.typeSignature <:< targetType))
        throw new IllegalArgumentException(s"Object $objectName is not instance of $targetType")

      val moduleMirror = rm.reflectModule(module)
      moduleMirror.instance.asInstanceOf[T]
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception => throw new IllegalArgumentException(s"Object $objectName not available", e)
    }
  }
}
