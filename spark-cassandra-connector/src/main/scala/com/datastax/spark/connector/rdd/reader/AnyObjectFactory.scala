package com.datastax.spark.connector.rdd.reader

import java.lang.reflect.Constructor

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.reflect.ConstructorUtils
import com.datastax.spark.connector.util.Logging
import com.google.common.primitives.Primitives
import com.thoughtworks.paranamer.AdaptiveParanamer

import com.datastax.spark.connector.util.Reflect

/** Factory for creating objects of any type by invoking their primary constructor.
  * Unlike Java reflection Methods or Scala reflection Mirrors, this factory is serializable
  * and can be safely passed along with Spark tasks. */
class AnyObjectFactory[T: TypeTag] extends Logging with Serializable {

  import AnyObjectFactory._

  @transient
  private val tpe = implicitly[TypeTag[T]].tpe

  @transient
  lazy val rm: RuntimeMirror =
    runtimeMirror(Thread.currentThread().getContextClassLoader)

  // This must be serialized:
  val javaClass: Class[T] =
    rm.runtimeClass(tpe).asInstanceOf[Class[T]]

  // This must be serialized:
  val constructorParamTypeNames: IndexedSeq[ParamType] =
    toParamTypeNames(resolveConstructor(javaClass))

  @transient
  private lazy val javaConstructor: Constructor[T] =
    resolveConstructorFromParamTypeNames(javaClass, constructorParamTypeNames)

  // It is quite important to invoke constructorParamTypes here because it invokes client side validation whether
  // the right constructor exists or not, before the job is started
  val argCount: Int = constructorParamTypes.length

  val argOffset: Int = oneIfMemberClass(javaClass)

  @transient
  lazy val constructorParamTypes: Array[Type] = {
    val requiredParamClasses = javaConstructor.getParameterTypes
      .drop(AnyObjectFactory.oneIfMemberClass(javaClass))

    Reflect.constructor(tpe).asTerm.alternatives.map { term =>
      val ctorSymbol = term.asMethod
      val ctorType = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
      val ctorParams = ctorType.params.map(_.asTerm.typeSignature).toArray

      // this is required because if javaClass is a Java style member class, ctorParams includes a param for
      // a reference to the outer class instance; this doesn't happen in case of Scala style member classes
      if (ctorParams.headOption.exists(t => Some(rm.runtimeClass(t)) == getRealEnclosingClass(javaClass)))
        ctorParams.drop(1) else ctorParams
    }.find(checkIfTypesApplyToClasses(requiredParamClasses)).get
  }

  private def checkIfTypesApplyToClasses(requiredParamClasses: Array[Class[_]])(providedParams: Array[Type]) = {
    val paramClasses = providedParams.map(t => rm.runtimeClass(t): Class[_])

    paramClasses.length == requiredParamClasses.length && paramClasses.zip(requiredParamClasses).forall {
      case (pc, rpc) =>
        if (!rpc.isPrimitive && pc.isPrimitive)
          rpc.isAssignableFrom(Primitives.wrap(pc))
        else
          rpc.isAssignableFrom(pc)
    }
  }

  @transient
  private lazy val argBuffer = {
    val buffer = Array.ofDim[AnyRef](argOffset + argCount)
    if (isRealMemberClass(javaClass)) {
      buffer(0) = resolveDirectOuterInstance()
    }
    buffer
  }

  private def resolveDirectOuterInstance() = {
    // this will create a collection of outer classes, where the first one is a top level class
    val outerClasses = extractOuterClasses(javaClass).reverse

    // create an instance of the top level class
    val rootInstance = outerClasses.head.newInstance().asInstanceOf[AnyRef]

    // this is for Spark Shell - all classes created in the console are inner classes of some mysterious Spark classes;
    // it shows that we need to create only the instance of the top level class, and then, instances of all inner
    // classes are created automatically and stored in magic $iw field; the aim of dive function is to retrieve a value
    // of the most deeply nested $iw field - it should be the instance of the direct enclosing class of javaClass
    val deepestCreatedInstance = dive(rootInstance.asInstanceOf[AnyRef])

    // now we drop the classes for which we already have an instance
    val remainingClasses = outerClasses.dropWhile(_ != deepestCreatedInstance.getClass).drop(1)

    // and create instances of the rest
    remainingClasses.foldLeft(deepestCreatedInstance)((outerInstance, innerClass) =>
      ConstructorUtils.invokeExactConstructor(innerClass, outerInstance).asInstanceOf[AnyRef])
  }

  def newInstance(args: AnyRef*): T = {
    for (i <- 0 until argCount)
      argBuffer(i + argOffset) = args(i)
    javaConstructor.newInstance(argBuffer: _*)
  }
}

object AnyObjectFactory extends Logging {
  private[connector] type ParamType = Either[Class[_], String]

  private[connector] val paranamer = new AdaptiveParanamer

  private[connector] def getDefaultConstructor[T](clazz: Class[T]): Constructor[T] = {
    val ctor = clazz.getConstructors.maxBy(_.getParameterTypes.length)
    paranamer.lookupParameterNames(ctor)
    ctor.asInstanceOf[Constructor[T]]
  }

  private[connector] def getNoArgsConstructor[T](clazz: Class[T]): Constructor[T] = {
    getRealEnclosingClass(clazz).fold(clazz.getConstructor())(clazz.getConstructor(_))
  }

  private[connector] def resolveConstructor[T](clazz: Class[T]): Constructor[T] = {
    lazy val defaultCtor = Try {
      val ctor = getDefaultConstructor(clazz)
      logDebug(s"Using a default constructor ${ctor.getParameterTypes.map(_.getSimpleName)} for ${clazz.getName}")
      ctor
    }

    lazy val noArgsCtor = Try {
      val ctor = getNoArgsConstructor(clazz)
      logDebug(s"Using a no-args constructor for ${clazz.getName}")
      ctor
    }

    defaultCtor.orElse(noArgsCtor).getOrElse(
      throw new NoSuchMethodException(s"Cannot resolve any suitable constructor for class ${clazz.getName}"))
  }

  def oneIfMemberClass(clazz: Class[_]) =
    if (isRealMemberClass(clazz)) 1 else 0

  def isNoArgsConstructor(ctor: Constructor[_]) =
    ctor.getParameterTypes.length == oneIfMemberClass(ctor.getDeclaringClass)

  def resolveConstructorFromParamTypeNames[T](clazz: Class[T], paramTypeNames: IndexedSeq[ParamType]): Constructor[T] = {
     clazz.getConstructors.find(toParamTypeNames(_) == paramTypeNames).get.asInstanceOf[Constructor[T]]
  }

  private[connector] def toParamTypeNames(ctor: Constructor[_]): IndexedSeq[ParamType] = {
    ctor.getParameterTypes.map(c => if (c.isPrimitive) Right(c.getName) else Left(c)).toIndexedSeq
  }

  private[connector] def extractOuterClasses(c: Class[_]): List[Class[_]] = {
    getRealEnclosingClass(c) match {
      case Some(enclosingClass) => enclosingClass :: extractOuterClasses(enclosingClass)
      case None => Nil
    }
  }

  private[connector] def dive(instance: AnyRef): AnyRef = {
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

  /**
   * This method checks if the class is a member class which requires providing a reference to the enclosing in its
   * constructors. We cannot just check it by invoking `java.lang.Class#isMemberClass` because it
   * will return `true` for classes enclosed in Scala objects. They do not accept reference to enclosing
   * class in their constructors, and therefore they need to be treated as normal, top level classes.
   */
  def isRealMemberClass[T](clazz: Class[T]) = {
    clazz.isMemberClass &&
      clazz.getConstructors.headOption.exists(_.getParameterTypes.headOption.exists(_ == clazz.getEnclosingClass))
  }

  /**
   * Returns an enclosing class wrapped by `Option`. It returns `Some` if
   * [[com.datastax.spark.connector.rdd.reader.AnyObjectFactory#isRealMemberClass isRealMemberClass]] returns `true`.
   */
  def getRealEnclosingClass[T](clazz: Class[T]) = if (isRealMemberClass(clazz)) Some(clazz.getEnclosingClass) else None

}

