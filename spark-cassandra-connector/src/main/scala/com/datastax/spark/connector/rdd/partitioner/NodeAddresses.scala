package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import scala.collection.JavaConversions._

import com.datastax.spark.connector.cql.CassandraConnector

/** Looks up listen address of a cluster node given its RPC address.
  * Uses system.peers table as the source of information.
  * If such information for a node is missing, it assumes its listen
  * address equals its RPC address */
class NodeAddresses(conn: CassandraConnector) extends Serializable {

  /** Maps rpc addresses to listen addresses for every cluster node.
    * If rpc address is not known, returns the same address. */
  lazy val rpcToListenAddress: InetAddress => InetAddress = {
    conn.withSessionDo { session =>
      val table = "system.peers"
      val rpcAddressColumn = "rpc_address"
      val listenAddressColumn = "peer"

      // TODO: fetch information about the local node from system.local, when CASSANDRA-9436 is done
      val rs = session.execute(s"SELECT $rpcAddressColumn, $listenAddressColumn FROM $table")
      for {
        row <- rs.all()
        rpcAddress <- Option(row.getInet(rpcAddressColumn))
        listenAddress = row.getInet(listenAddressColumn)
      } yield (rpcAddress, listenAddress)
    }.toMap.withDefault(identity)
  }
  
  /** Returns a list of IP-addresses and host names that identify a node.
    * Useful for giving Spark the list of preferred nodes for the Spark partition. */
  def hostNames(rpcAddress: InetAddress): Set[String] = {
    val listenAddress = rpcToListenAddress(rpcAddress)
    Set(
      rpcAddress.getHostAddress, 
      rpcAddress.getHostName,
      listenAddress.getHostAddress,
      listenAddress.getHostName
    )
  }
}
