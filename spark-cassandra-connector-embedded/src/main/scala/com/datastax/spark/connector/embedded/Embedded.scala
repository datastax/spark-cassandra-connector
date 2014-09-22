package com.datastax.spark.connector.embedded

/** INTERNAL API.
  * Helper class for demos and tests. Useful for quick prototyping. */
private[embedded] trait Embedded extends EmbeddedIO with Serializable with Assertions


private[embedded] trait EmbeddedIO {

  import java.io._
  import java.net.{InetAddress, Socket}
  import java.util.UUID

  import scala.io.Source
  import scala.language.reflectiveCalls
  import scala.util.Try
  import com.google.common.io.Files
  import org.apache.commons.lang3.SystemUtils

  val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
    * Similar to try-with-resources in Java 7. */
  def closeAfterUse[T, C <: { def close() }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

  /** Copies a text file substituting every occurrence of `$ {VARIABLE}` with a value from the given map */
  def copyTextFileWithVariableSubstitution(source: InputStream, target: OutputStream, map: String => String) {
    val regex = "\\$\\{([a-zA-Z0-9_]+)\\}".r
    closeAfterUse(new PrintWriter(target)) { writer =>
      val input = Source.fromInputStream(source, "UTF-8")
      for (line <- input.getLines()) {
        val substituted = regex.replaceAllIn(line, m => map(m.group(1)))
        writer.println(substituted)
      }
    }
  }

  def createTempDir: File = {
    val dir = mkdir(new File(Files.createTempDir(), "spark-tmp-" + UUID.randomUUID.toString))
    registerShutdownDeleteDir(dir)

    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        if (! hasRootAsShutdownDeleteDir(dir)) deleteRecursively(dir)
      }
    })
    dir
  }

  /** Makes a new directory or throws an `IOException` if it cannot be made */
  def mkdir(dir: File): File = {
    if (!dir.mkdir()) throw new IOException(s"Could not create dir $dir")
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


  def registerShutdownDeleteDir(file: File) {
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += file.getAbsolutePath
    }
  }

  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
  }

  def deleteRecursively(file: File) {
    if (file != null) {
      if (file.isDirectory && !isSymlink(file)) {
        for (child <- listFilesSafely(file))
          deleteRecursively(child)
      }
      if (!file.delete()) {
        if (file.exists())
          throw new IOException("Failed to delete: " + file.getAbsolutePath)
      }
    }
  }

  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (SystemUtils.IS_OS_WINDOWS) return false
    val fcd = if (file.getParent == null) file else new File(file.getParentFile.getCanonicalFile, file.getName)
    if (fcd.getCanonicalFile.equals(fcd.getAbsoluteFile)) false else true
  }

  def listFilesSafely(file: File): Seq[File] = {
    val files = file.listFiles()
    if (files == null) throw new IOException("Failed to list files for dir: " + file)
    files
  }
}

