package com.datastax.spark.connector.embedded

import java.net.InetAddress

import org.apache.commons.configuration.ConfigurationException

object UserDefinedProperty {

  trait TypedProperty {
    type ValueType

    def convertValueFromString(str: String): ValueType

    def checkValueType(obj: Any): ValueType
  }

  trait IntProperty extends TypedProperty {
    type ValueType = Int

    def convertValueFromString(str: String) = str.toInt

    def checkValueType(obj: Any) =
      obj match {
        case x: Int => x
        case _ => throw new ClassCastException(s"Expected Int but found ${obj.getClass.getName}")
      }
  }

  trait InetAddressProperty extends TypedProperty {
    type ValueType = InetAddress

    def convertValueFromString(str: String) = InetAddress.getByName(str)

    def checkValueType(obj: Any) =
      obj match {
        case x: InetAddress => x
        case _ => throw new ClassCastException(s"Expected InetAddress but found ${obj.getClass.getName}")
      }
  }

  abstract sealed class NodeProperty(val propertyName: String) extends TypedProperty

  case object HostProperty extends NodeProperty("IT_TEST_CASSANDRA_HOSTS") with InetAddressProperty

  case object PortProperty extends NodeProperty("IT_TEST_CASSANDRA_PORTS") with IntProperty

  private def getValueSeq(propertyName: String): Seq[String] = {
    sys.env.get(propertyName) match {
      case Some(p) => p.split(",").map(e => e.trim).toIndexedSeq
      case None => IndexedSeq()
    }
  }

  private def getValueSeq(nodeProperty: NodeProperty): Seq[nodeProperty.ValueType] =
    getValueSeq(nodeProperty.propertyName).map(x => nodeProperty.convertValueFromString(x))

  val hosts = getValueSeq(HostProperty)
  val ports = getValueSeq(PortProperty)

  def getProperty(nodeProperty: NodeProperty): Option[String] =
    sys.env.get(nodeProperty.propertyName)

  def getPropertyOrThrowIfNotFound(nodeProperty: NodeProperty): String =
    getProperty(nodeProperty).getOrElse(
      throw new ConfigurationException(s"Missing ${nodeProperty.propertyName} in system environment"))
}
