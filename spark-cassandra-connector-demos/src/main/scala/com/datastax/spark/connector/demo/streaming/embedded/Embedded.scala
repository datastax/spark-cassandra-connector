package com.datastax.spark.connector.demo.streaming.embedded

import java.io.{IOException, File}
import java.util.UUID

import com.datastax.spark.connector.demo.Assertions
import com.google.common.io.Files
import org.apache.commons.lang3.SystemUtils
import com.datastax.spark.connector.util.{Logging, IOUtils}

/** INTERNAL API.
  * Helper class for demos and tests. Useful for quick prototyping. */
private[embedded] trait Embedded extends Serializable with Assertions with Logging {

  val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    val dir = IOUtils.mkdir(new File(Files.createTempDir(), "spark-tmp-" + UUID.randomUUID.toString))

    registerShutdownDeleteDir(dir)

    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        if (! hasRootAsShutdownDeleteDir(dir)) deleteRecursively(dir)
      }
    })
    dir
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
