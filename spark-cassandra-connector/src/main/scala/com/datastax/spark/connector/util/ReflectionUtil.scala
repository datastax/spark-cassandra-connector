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

  /** Returns a list of parameter names and types of the main constructor.
    * The main constructor is assumed to be the one that has the highest number of parameters.
    * In case on ambiguity, this method throws IllegalArgumentException.*/
  def constructorParams(tpe: Type): Seq[(String, Type)] = TypeTag.synchronized {
    val constructors = tpe.declaration(nme.CONSTRUCTOR).asTerm.alternatives.map(_.asMethod)
    val paramCount = constructors.map(_.paramss.flatten.size).max
    val ctorSymbol = constructors.filter(_.paramss.flatten.size == paramCount) match {
      case List(onlyOne) => onlyOne
      case _             => throw new IllegalArgumentException(
        "Multiple constructors with the same number of parameters not allowed.")
    }
    // the reason we're using typeSignatureIn is because the constructor might be a generic type
    // and we don't really want to get generic type parameters here, but concrete ones:
    val ctorMethod = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    for (param <- ctorMethod.params) yield
      (param.name.toString, param.typeSignature)
  }

  def constructorParams[T : TypeTag]: Seq[(String, Type)] = TypeTag.synchronized {
    constructorParams(implicitly[TypeTag[T]].tpe)
  }

  /** Returns a list of names and return types of 0-argument public methods of a Scala type */
  def getters(tpe: Type): Seq[(String, Type)] = TypeTag.synchronized {
    val methods = for (d <- tpe.declarations.toSeq if d.isMethod && d.isPublic) yield d.asMethod
    val getters = for (m <- methods if m.paramss.size == 0 && m.typeParams.size == 0) yield m
    for (g <- getters) yield {
      // the reason we're using typeSignatureIn is because the getter might be a generic type
      // and we don't really want to get generic type here, but a concrete one:
      val returnType = g.typeSignatureIn(tpe).asInstanceOf[NullaryMethodType].resultType
      (g.name.toString, returnType)
    }
  }

  def getters[T : TypeTag]: Seq[(String, Type)] = TypeTag.synchronized {
    getters(implicitly[TypeTag[T]].tpe)
  }

  /** Returns a list of names and parameter types of 1-argument public methods of a Scala type,
    * returning no result (Unit) */
  def setters(tpe: Type): Seq[(String, Type)] = TypeTag.synchronized {
    val methods = for (d <- tpe.declarations.toSeq if d.isMethod && d.isPublic) yield d.asMethod
    val setters = for (m <- methods if m.paramss.size == 1 && m.paramss(0).size == 1 && m.returnType =:= typeOf[Unit]) yield m
    for (s <- setters) yield {
      // need a concrete type, not a generic one:
      val paramType = s.typeSignatureIn(tpe).asInstanceOf[MethodType].params(0).typeSignature
      (s.name.toString, paramType)
    }
  }

  def setters[T : TypeTag]: Seq[(String, Type)] = TypeTag.synchronized {
    setters(implicitly[TypeTag[T]].tpe)
  }


}
