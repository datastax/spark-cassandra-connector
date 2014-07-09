package com.datastax.driver.spark.util


import com.datastax.driver.spark.{RDDFunctions, SparkContextFunctions}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.api._
import scala.reflect.runtime.universe._
import scala.reflect.api.{Mirror, TypeCreator}

/**
 * Some helper 
 */
object JavaApiHelper {

  def getTypeTag[T](clazz: Class[T]): Universe#TypeTag[T] = {
    TypeTag.apply(runtimeMirror(Thread.currentThread().getContextClassLoader), new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U # Type ={
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }
  
  def getSparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)
  
  def getRDDFunctions[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions[T](rdd)
  
}
