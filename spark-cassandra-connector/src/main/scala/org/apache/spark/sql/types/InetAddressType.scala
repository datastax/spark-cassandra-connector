package org.apache.spark.sql.types

import java.net.InetAddress

import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock

/**
 * :: DeveloperApi ::
 *
 * The data type representing `InetAddress` values.
 *
 * @group dataType
 */
@DeveloperApi
class InetAddressType private() extends NativeType with PrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "InetAddressType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type JvmType = InetAddress
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }

  // Convert to string to compare InetAddress because it doesn't support comparison.
  // It's not a good solution though.
  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: InetAddress, y: InetAddress) = x.toString.compareTo(y.toString)
  }
  /**
   * The default size of a value of the StringType is 4096 bytes.
   */
  override def defaultSize: Int = 16

  private[spark] override def asNullable: InetAddressType = this
}

case object InetAddressType extends InetAddressType