package com.datastax.driver.spark.util

import java.io._
import java.net.{Socket, InetAddress}

import scala.io.Source
import scala.language.reflectiveCalls
import scala.util.Try

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

  /** Copies a text file substituting every occurrence of ${VARIABLE} with a value from the given map */
  def copyTextFileWithVariableSubstitution(source: InputStream, target: OutputStream, map: String => String) {
    val regex = "\\$\\{([a-zA-Z0-9_]+)\\}".r
    IOUtils.closeAfterUse(new PrintWriter(target)) { writer =>
      val input = Source.fromInputStream(source, "UTF-8")
      for (line <- input.getLines()) {
        val substituted = regex.replaceAllIn(line, m => map(m.group(1)))
        writer.println(substituted)
      }
    }
  }

  /** Makes a new directory or throws an `IOException` if it cannot be made */
  def mkdir(dir: File): File = {
    if (!dir.mkdir())
      throw new IOException(s"Could not create dir $dir")
    dir
  }


  /** Waits until a port at the given address is open or timeout passes.
    * @return true if managed to connect to the port, false if timeout happened first */
  def waitForPortOpen(host: InetAddress, port: Int, timeout: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    val portProbe = Iterator.continually {
      Try {
        Thread.sleep(100)
        val socket = new Socket(host, port)
        socket.close()
      }
    }
    portProbe
      .dropWhile(p => p.isFailure && System.currentTimeMillis() - startTime < timeout)
      .next()
      .isSuccess
  }

}
