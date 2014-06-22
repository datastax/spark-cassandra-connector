package com.datastax.driver.spark.util

import scala.language.reflectiveCalls

object IOUtils {

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
    * Similar to try-with-resources in Java 7. */
  def closeAfterUse[T, C <: { def close() }](closeable: C)(code: C => T): T = {
    try {
      code(closeable)
    }
    finally {
      closeable.close()
    }
  }

}
