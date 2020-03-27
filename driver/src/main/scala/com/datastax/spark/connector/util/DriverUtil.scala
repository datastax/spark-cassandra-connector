package com.datastax.spark.connector.util

import java.net.{InetAddress, InetSocketAddress}
import java.util.Optional

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.metadata.Node

object DriverUtil {

  //TODO use CqlIdentifier instead? Use implicit conversion to String? To internal string?
  def toName(id: CqlIdentifier): String = id.asInternal()

  def toOption[T](optional: Optional[T]): Option[T] =
    if (optional.isPresent) Some(optional.get()) else None

  def toAddress(node: Node): Option[InetSocketAddress] = {
    node.getEndPoint.resolve() match {
      case address: InetSocketAddress => if (address.isUnresolved) {
        Option(new InetSocketAddress(address.getHostString, address.getPort))
      } else {
        Option(address)
      }
      case _ => toOption(node.getBroadcastAddress)
    }
  }
}
