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

  // Because this new type is not a Spark internal supported data type, there is no
  // CAST method to convert a InetAddress to a String. Spark internally converts it
  // to Double, so it errors out the query such as
  //    select * from table where inet_address_column = '/74.125.239.135'
  // The correct query should be
  //    select * from table where CAST(inet_address_column as string) = '/74.125.239.135'
  //
  // The following code provides only a reference ordering implementation. But it will
  // never be called by Spark, for there is no CAST function or UDF to convert String
  // to InetAddress
  //
  // Convert to host address to compare InetAddress because it doesn't support comparison.
  // It's not a good solution though.
  import Ordering.Implicits._
  def unsignedByte(x: Byte) = (x + 256) % 256
  def address(inet: InetAddress): Iterable[Int] =
    inet.getAddress.map(unsignedByte)
  val ord = Ordering by address
  private[sql] val ordering = ord
  /**
   * The default size of a value of the InetAddressType is 16 bytes.
   */
  override def defaultSize: Int = 16

  private[spark] override def asNullable: InetAddressType = this
}

case object InetAddressType extends InetAddressType