package com.datastax.spark.connector.demo.streaming

import java.io.{IOException, File}
import java.util.UUID

import com.datastax.spark.connector.util.IOUtils
import com.google.common.io.Files
import org.apache.commons.lang3.SystemUtils

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.{Actor, PoisonPill}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.Logging
import com.datastax.spark.connector.demo.DemoApp

/**
 * Base helper trait for all streaming demos.
 */
trait StreamingDemo extends DemoApp {

  val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  def waitUntilTrue(condition: () => Boolean, waitTime: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return true
      if (System.currentTimeMillis() > startTime + waitTime)
        return false
      Thread.sleep(waitTime.min(100L))
    }
    throw new RuntimeException("unexpected error")
  }

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

  private[connector] def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (SystemUtils.IS_OS_WINDOWS) return false
    val fcd = if (file.getParent == null) file else new File(file.getParentFile.getCanonicalFile, file.getName)
    if (fcd.getCanonicalFile.equals(fcd.getAbsoluteFile)) false else true
  }

  private[connector] def listFilesSafely(file: File): Seq[File] = {
    val files = file.listFiles()
    if (files == null) throw new IOException("Failed to list files for dir: " + file)
    files
  }
}


/* Initializes Akka, Cassandra and Spark settings. */
final class SparkCassandraSettings(rootConfig: Config) {
  def this() = this(ConfigFactory.load)

  protected val config = rootConfig.getConfig("spark-cassandra")

  val SparkMaster: String = config.getString("spark.master")

  val SparkCleanerTtl: Int = config.getInt("spark.cleaner.ttl")

  val CassandraSeed: String = config.getString("spark.cassandra.connection.host")

  val SparkStreamingBatchDuration: Long = config.getLong("spark.streaming.batch.duration")
}

trait CounterActor extends Actor  with Logging {

  protected val scale = 30

  private var count = 0

  protected def increment(): Unit = {
    count += 1
    if (count == scale) self ! PoisonPill
  }
}

private[demo] object InternalStreamingEvent {
  sealed trait Status extends Serializable
  case class Pushed(data: AnyRef) extends Status
  case object Completed extends Status
  case object Report extends Status
  case class WordCount(word: String, count: Int)
}

/** When called upon, the Reporter starts a task which checks at regular intervals whether
  * the produced amount of data has all been written to Cassandra from the stream. This allows
  * the demo to stop on its own once this assertion is true. It will stop the task and ping
  * the `NodeGuardian`, its supervisor, of the `Completed` state.
  */
class Reporter(ssc: StreamingContext, keyspaceName: String, tableName: String, data: immutable.Set[String]) extends CounterActor  {
  import akka.actor.Cancellable
  import com.datastax.spark.connector.streaming._
  import InternalStreamingEvent._
  import context.dispatcher

  private var task: Option[Cancellable] = None

  def receive: Actor.Receive = {
    case Report => report()
  }

  def done: Actor.Receive = {
    case Completed => complete()
  }

  def report(): Unit = {
    task = Some(context.system.scheduler.schedule(Duration.Zero, 1.millis) {
      val rdd = ssc.cassandraTable[WordCount](keyspaceName, tableName).select("word", "count")
      if (rdd.collect.nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2) {
        context.become(done)
        self ! Completed
      }
    })
  }

  def complete(): Unit = {
    task map (_.cancel())
    val rdd = ssc.cassandraTable[WordCount](keyspaceName, tableName).select("word", "count")
    assert(rdd.collect.length == data.size)
    log.info(s"Saved data to Cassandra.")
    context.parent ! Completed
  }
}
