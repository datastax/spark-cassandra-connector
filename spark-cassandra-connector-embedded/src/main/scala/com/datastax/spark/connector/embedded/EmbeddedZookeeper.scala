package com.datastax.spark.connector.embedded

import java.net.InetSocketAddress

import scala.util.Try
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

/** Implements a simple standalone ZooKeeperServer.
  * To create a ZooKeeper client object, the application needs to pass a
  * connection string containing a comma separated list of host:port pairs,
  * each corresponding to a ZooKeeper server.
  * <p>
  * Session establishment is asynchronous. This constructor will initiate
  * connection to the server and return immediately - potentially (usually)
  * before the session is fully established. The watcher argument specifies
  * the watcher that will be notified of any changes in state. This
  * notification can come at any point before or after the constructor call
  * has returned.
  * <p>
  * The instantiated ZooKeeper client object will pick an arbitrary server
  * from the connectString and attempt to connect to it. If establishment of
  * the connection fails, another server in the connect string will be tried
  * (the order is non-deterministic, as we random shuffle the list), until a
  * connection is established. The client will continue attempts until the
  * session is explicitly closed.
  *
  * @param connectString comma separated host:port pairs, each corresponding to a zk
  *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
  *            the optional chroot suffix is used the example would look
  *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
  *            where the client would be rooted at "/app/a" and all paths
  *            would be relative to this root - ie getting/setting/etc...
  *            "/foo/bar" would result in operations being run on
  *            "/app/a/foo/bar" (from the server perspective).
  *            Default: the local IP and default port: 2180.
  */
class EmbeddedZookeeper(val connectString: String = ZookeeperConnectionString) extends Embedded {

  val snapshotDir = createTempDir

  val logDir = createTempDir

  val server = new ZooKeeperServer(snapshotDir, logDir, 500)

  val (ip, port) = {
    val splits = connectString.split(":")
    (splits(0), splits(1).toInt)
  }

  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress(ip, port), 16)
  factory.startup(server)
  println(s"ZooKeeperServer isRunning: $isRunning")

  def isRunning: Boolean = Try(server.isRunning) getOrElse false

  def shutdown(): Unit = {
    println(s"Shutting down ZK NIOServerCnxnFactory.")
    factory.shutdown()
    deleteRecursively(snapshotDir)
    deleteRecursively(logDir)
  }
}