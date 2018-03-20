package org.apache.spark.sql.catalyst

object ReflectionLock {

  protected object ScalaReflectionLockFallback

  val SparkReflectionLock = ScalaReflectionLockFallback
}
