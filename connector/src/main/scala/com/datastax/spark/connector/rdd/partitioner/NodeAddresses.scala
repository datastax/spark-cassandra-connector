package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import scala.collection.JavaConversions._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util.DriverUtil.{toName, toOption}

/** Looks up listen address of a cluster node given its Native Transport address.
  * Uses system.peers table as the source of information.
  * If such information for a node is missing, it assumes its listen
  * address equals its RPC address */
class NodeAddresses(conn: CassandraConnector) extends Serializable {

  /** Maps native transport addresses to listen addresses for every cluster node.
    * If native transport address is not known, returns the same address. */
  lazy val nativeTransportAddressToListenAddress: InetAddress => InetAddress = {
    conn.withSessionDo { session =>
      val table = "system.peers"
      val listenAddressColumnName = "peer"

      val transportAddressColumn = toOption(session.getMetadata.getKeyspace("system"))
        .flatMap(k => toOption(k.getTable("peers")))
        .flatMap(t => toOption(t.getColumn("native_transport_address")))

      val nativeTransportAddressColumnName = transportAddressColumn.map(c => toName(c.getName)).getOrElse("rpc_address")

      // TODO: fetch information about the local node from system.local, when CASSANDRA-9436 is done
      val rs = session.execute(s"SELECT $nativeTransportAddressColumnName, $listenAddressColumnName FROM $table")
      for {
        row <- rs.all()
        nativeTransportAddress <- Option(row.getInetAddress(nativeTransportAddressColumnName))
        listenAddress = row.getInetAddress(listenAddressColumnName)
      } yield (nativeTransportAddress, listenAddress)
    }.toMap.withDefault(identity)
  }

  /** Returns a list of IP-addresses and host names that identify a node.
    * Useful for giving Spark the list of preferred nodes for the Spark partition. */
  def hostNames(nativeTransportAddress: InetAddress): Set[String] = {
    val listenAddress = nativeTransportAddressToListenAddress(nativeTransportAddress)
    Set(
      nativeTransportAddress.getHostAddress,
      nativeTransportAddress.getHostName,
      listenAddress.getHostAddress,
      listenAddress.getHostName
    )
  }
}
