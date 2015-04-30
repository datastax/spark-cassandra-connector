package org.apache.spark

import java.io.File

import org.apache.spark.util.Utils

/** Access Spark package private Utils object */
object Util {

  def deleteRecursively(file: File) : Unit = {
    Utils.deleteRecursively(file)
  }
}
