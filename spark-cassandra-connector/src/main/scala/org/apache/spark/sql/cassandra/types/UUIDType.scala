package org.apache.spark.sql.cassandra.types

import scala.reflect.runtime.universe.typeTag

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.types.{PrimitiveType, NativeType}

/**
 * :: DeveloperApi ::
 *
 * The data type representing `UUID` values.
 *
 * @group dataType
 */
@DeveloperApi
class UUIDType private() extends NativeType with PrimitiveType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "UUIDType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type JvmType = UUID
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: UUID, y: UUID) = x.compareTo(y)
  }
  /**
   * The default size of a value of the UUIDType is 16 bytes.
   */
  override def defaultSize: Int = 16

  private[spark] override def asNullable: UUIDType = this
}

case object UUIDType extends UUIDType