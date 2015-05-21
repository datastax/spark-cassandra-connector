package org.apache.spark.sql.cassandra.types


import scala.reflect.runtime.universe.typeTag

import java.net.InetAddress

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.types.{PrimitiveType, NativeType}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `InetAddress` values.
 *
 * @group dataType
 */
@DeveloperApi
class InetAddressType private() extends NativeType with PrimitiveType with Logging {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "InetAddressType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type JvmType = InetAddress
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }

  // Convert to host address to compare InetAddress because it doesn't support comparison.
  // It's not a good solution though.
  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: InetAddress, y: InetAddress) = {
      logWarning("InetAddress doesn't support comparison. Convert it to host address to compare.")
      x.getHostAddress.compareTo(y.getHostAddress)
    }
  }
  /**
   * The default size of a value of the InetAddressType is 16 bytes.
   */
  override def defaultSize: Int = 16

  private[spark] override def asNullable: InetAddressType = this
}

case object InetAddressType extends InetAddressType