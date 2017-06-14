package com.datastax.spark.connector.embedded

import java.net.{ConnectException, Socket}
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

case class DynamicCassandraPorts(basePort: Int) extends CassandraPorts {

  import DynamicCassandraPorts._

  def getRpcPort(index: Int): Int = basePort + RPC_OFFSET + index

  def getJmxPort(index: Int): Int = basePort + JMX_OFFSET + index

  def getSslStoragePort(index: Int): Int = basePort + SSL_STORAGE_OFFSET + index

  def getStoragePort(index: Int): Int = basePort + STORAGE_OFFSET + index

  def release(): Unit = DynamicCassandraPorts.releaseBasePort(basePort)
}


object DynamicCassandraPorts {
  val MaxInstances = 5

  val RPC_OFFSET = 0
  val JMX_OFFSET = MaxInstances
  val SSL_STORAGE_OFFSET = MaxInstances * 2
  val STORAGE_OFFSET = MaxInstances * 3

  private lazy val basePath = Paths.get(sys.props("baseDir"), "target", "ports")

  private lazy val basePorts = 10000 until 20000 by 50

  private def portLockFile(port: Int) = basePath.resolve(s"port_$port.lock")

  def apply(): DynamicCassandraPorts = DynamicCassandraPorts(chooseBasePort())

  def areAllPortsAvailable(host: String, ports: Seq[Int]): Boolean = {
    ports.par.forall { port =>
      Try(new Socket(host, port)) match {
        case Success(socket) =>
          socket.close()
          false
        case Failure(ex: ConnectException) =>
          true
        case _ =>
          false
      }
    }
  }

  @tailrec
  def chooseBasePort(): Int = {
    Files.createDirectories(basePath)
    val foundPort = basePorts.find { basePort =>
      val file = portLockFile(basePort)
      Try(Files.createFile(file)) match {
        case Success(filePath) =>
          require(filePath == file)
          if (areAllPortsAvailable("127.0.0.1", basePort until (basePort + 100))) {
            true
          } else {
            releaseBasePort(basePort)
            false
          }
        case Failure(ex: FileAlreadyExistsException) =>
          false
        case Failure(ex) =>
          throw ex
      }
    }

    if (foundPort.isDefined) foundPort.get
    else {
      Thread.sleep(1000)
      chooseBasePort()
    }
  }

  def releaseBasePort(basePort: Int): Unit = {
    val file = portLockFile(basePort)
    Files.deleteIfExists(file)
  }

}
