package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import scala.collection.JavaConversions._
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.reflect.runtime.universe._

import org.apache.cassandra.utils.ByteBufferUtil
import org.joda.time.DateTime


class TypeConversionException(val message: String, cause: Exception = null) extends Exception(message, cause)

/** Machinery for converting objects of any type received from Cassandra into objects of Scala types.
  * Every converter knows how to convert object to one type. See `TypeConverter`
  * companion object for a list of available converters. */
trait TypeConverter[T] extends Serializable {
  /** Compile time type of the converter target */
  type targetType = T
  
  /** String representation of the converter target type */
  def targetTypeString: String

  /** Converts an object into `T`. */
  def convert(obj: Any): T
}

/** Defines a set of converters and implicit functions used to look up an appropriate converter for
  * a desired type. Thanks to implicit method lookup, it is possible to implement a generic
  * method `CassandraRow#get`, which picks up the right converter basing solely on its type argument. */
object TypeConverter {

  implicit object AnyConverter extends TypeConverter[Any] {
    def convert(obj: Any) = obj
    def targetTypeString = "Any"
  }

  implicit object AnyRefConverter extends TypeConverter[AnyRef] {
    def convert(obj: Any) = obj.asInstanceOf[AnyRef]
    def targetTypeString = "AnyRef"
  }

  implicit object BooleanConverter extends TypeConverter[Boolean] {
    def convert(obj: Any) = obj match {
      case x: java.lang.Boolean => x
      case x: java.lang.Integer => x != 0
      case x: java.lang.Long => x != 0L
      case x: java.math.BigInteger => x != java.math.BigInteger.ZERO
      case x: String => x.toBoolean
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "Boolean"
  }

  implicit object IntConverter extends TypeConverter[Int] {
    def convert(obj: Any) = obj match {
      case x: Number => x.intValue
      case x: String => x.toInt
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "Int"
  }

  implicit object LongConverter extends TypeConverter[Long] {
    def convert(obj: Any) = obj match {
      case x: Number => x.longValue
      case x: Date => x.getTime
      case x: String => x.toLong
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "Long"
  }

  implicit object FloatConverter extends TypeConverter[Float] {
    def convert(obj: Any) = obj match {
      case x: Number => x.floatValue
      case x: String => x.toFloat
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "Float"
  }

  implicit object DoubleConverter extends TypeConverter[Double] {
    def convert(obj: Any) = obj match {
      case x: Number => x.doubleValue
      case x: String => x.toDouble
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "Double"
  }

  implicit object StringConverter extends TypeConverter[String] {
    def convert(obj: Any) = obj match {
      case x: Date => TimestampFormatter.format(x)
      case x: Array[Byte] => "0x" + x.map("%02x" format _).mkString
      case x: Map[_, _] => x.map(kv => convert(kv._1) + ": " + convert(kv._2)).mkString("{", ",", "}")
      case x: Set[_] => x.map(convert).mkString("{", ",", "}")
      case x: Seq[_] => x.map(convert).mkString("[", ",", "]")
      case x => x.toString
    }
    def targetTypeString = "String"
  }

  implicit object ByteBufferConverter extends TypeConverter[ByteBuffer] {
    def convert(obj: Any) = obj match {
      case x: ByteBuffer => x
      case x: Array[Byte] => ByteBuffer.wrap(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "ByteBuffer"
  }

  implicit object ByteArrayConverter extends TypeConverter[Array[Byte]] {
    def convert(obj: Any) = obj match {
      case x: Array[Byte] => x
      case x: ByteBuffer => ByteBufferUtil.getArray(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "Array[Byte]"
  }

  implicit object DateConverter extends TypeConverter[Date] {
    def convert(obj: Any) = obj match {
      case x: Date => x
      case x: DateTime => x.toDate
      case x: Long => new Date(x)
      case x: UUID if x.version() == 1 => new Date(x.timestamp())
      case x: String => TimestampParser.parse(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "java.util.Date"
  }

  implicit object SqlDateConverter extends TypeConverter[java.sql.Date] {
    def convert(obj: Any) = obj match {
      case x: java.sql.Date => x
      case x: Date => new java.sql.Date(x.getTime)
      case x: DateTime => new java.sql.Date(x.toDate.getTime)
      case x: Long => new java.sql.Date(x)
      case x: UUID if x.version() == 1 => new java.sql.Date(x.timestamp())
      case x: String => new java.sql.Date(TimestampParser.parse(x).getTime)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "java.sql.Date"
  }

  implicit object JodaDateConverter extends TypeConverter[DateTime] {
    def convert(obj: Any) = obj match {
      case x: DateTime => x
      case x: Date => new DateTime(x)
      case x: Long => new DateTime(x)
      case x: UUID if x.version() == 1 => new DateTime(x.timestamp())
      case x: String => new DateTime(TimestampParser.parse(x))
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "org.joda.time.DateTime"
  }

  implicit object BigIntConverter extends TypeConverter[BigInt] {
    def convert(obj: Any) = obj match {
      case x: BigInt => x
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => BigInt(x)
      case x: java.lang.Long => BigInt(x)
      case x: String => BigInt(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "BigInt"
  }

  implicit object JavaBigIntegerConverter extends TypeConverter[java.math.BigInteger] {
    def convert(obj: Any) = obj match {
      case x: BigInt => x.bigInteger
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => new java.math.BigInteger(x.toString)
      case x: java.lang.Long => new java.math.BigInteger(x.toString)
      case x: String => new java.math.BigInteger(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "java.math.BigInteger"
  }

  implicit object BigDecimalConverter extends TypeConverter[BigDecimal] {
    override def convert(obj: Any) = obj match {
      case x: Number => BigDecimal(x.toString)
      case x: String => BigDecimal(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "BigDecimal"
  }

  implicit object JavaBigDecimalConverter extends TypeConverter[java.math.BigDecimal] {
    def convert(obj: Any) = obj match {
      case x: Number => new java.math.BigDecimal(x.toString)
      case x: String => new java.math.BigDecimal(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "java.math.BigDecimal"
  }

  implicit object UUIDConverter extends TypeConverter[UUID] {
    override def convert(obj: Any) = obj match {
      case x: UUID => x
      case x: String => UUID.fromString(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "java.util.UUID"

  }

  implicit object InetAddressConverter extends TypeConverter[InetAddress] {
    override def convert(obj: Any) = obj match {
      case x: InetAddress => x
      case x: String => InetAddress.getByName(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }
    def targetTypeString = "java.net.InetAddress"
  }

  class TupleConverter[K, V](implicit kc: TypeConverter[K], kv: TypeConverter[V])
    extends TypeConverter[(K, V)] {

    override def convert(obj: Any) = obj match {
      case (k, v) => (kc.convert(k), kv.convert(v))
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeString.")
    }

    def targetTypeString = s"(${kc.targetTypeString}, ${kv.targetTypeString})"

  }

  abstract class CollectionConverter[CC, T](implicit c: TypeConverter[T], bf: CanBuildFrom[T, CC])
    extends TypeConverter[CC] {

    private def newCollection(items: Iterable[Any]) = {
      val builder = bf()
      for (item <- items)
        builder += c.convert(item)
      builder.result()
    }

    override def convert(obj: Any) = obj match {
      case null => bf.apply().result()
      case x: java.util.List[_] => newCollection(x)
      case x: java.util.Set[_] => newCollection(x)
      case x: java.util.Map[_, _] => newCollection(x)
      case x: Iterable[_] => newCollection(x)
      case x => throw new TypeConversionException(s"Cannot convert $x to $targetTypeString.")
    }

    def collectionTypeString: String
    def targetTypeString = s"$collectionTypeString[${c.targetTypeString}]"
  }

  class OptionConverter[T](implicit c: TypeConverter[T]) extends TypeConverter[Option[T]] {
    override def convert(obj: Any) = obj match {
      case null => None
      case other => Some(c.convert(obj))
    }
    def targetTypeString = s"Option[${c.targetTypeString}]"
  }

  implicit def optionConverter[T : TypeConverter] =
    new OptionConverter[T]

  implicit def tupleConverter[K : TypeConverter, V : TypeConverter] =
    new TupleConverter[K, V]

  implicit def listConverter[T : TypeConverter] =
    new CollectionConverter[List[T], T] { def collectionTypeString = "List" }

  implicit def vectorConverter[T : TypeConverter] =
    new CollectionConverter[Vector[T], T] { def collectionTypeString = "Vector" }

  implicit def setConverter[T : TypeConverter] =
    new CollectionConverter[Set[T], T] { def collectionTypeString = "Set" }

  implicit def treeSetConverter[T : TypeConverter : Ordering] =
    new CollectionConverter[TreeSet[T], T] { def collectionTypeString = "TreeSet" }

  implicit def seqConverter[T : TypeConverter] =
    new CollectionConverter[Seq[T], T] { def collectionTypeString = "Seq" }

  implicit def indexedSeqConverter[T : TypeConverter] =
    new CollectionConverter[IndexedSeq[T], T] { def collectionTypeString = "IndexedSeq" }

  implicit def iterableConverter[T : TypeConverter] =
    new CollectionConverter[Iterable[T], T] { def collectionTypeString = "Iterable" }

  implicit def mapConverter[K : TypeConverter, V : TypeConverter] =
    new CollectionConverter[Map[K, V], (K, V)] { def collectionTypeString = "Map" }

  implicit def treeMapConverter[K: TypeConverter : Ordering, V : TypeConverter] =
    new CollectionConverter[TreeMap[K, V], (K, V)] { def collectionTypeString = "TreeMap" }

  /** Converts Scala Options to Java nullable references. Used when saving data to Cassandra. */
  class OptionToNullConverter(nestedConverter: TypeConverter[_]) extends TypeConverter[AnyRef] {
    override def convert(obj: Any) = obj match {
      case Some(x) => nestedConverter.convert(x).asInstanceOf[AnyRef]
      case None => null
      case null => null
      case x => nestedConverter.convert(x).asInstanceOf[AnyRef]
    }

    def targetTypeString = nestedConverter.targetTypeString
  }

  // Support for Java collections:
  implicit def javaListConverter[T : TypeConverter] =
    new CollectionConverter[java.util.List[T], T] { def collectionTypeString = "java.util.List" }

  implicit def javaArrayListConverter[T : TypeConverter] =
    new CollectionConverter[java.util.ArrayList[T], T] { def collectionTypeString = "java.util.ArrayList" }

  implicit def javaSetConverter[T : TypeConverter] =
    new CollectionConverter[java.util.Set[T], T] { def collectionTypeString = "java.util.Set" }

  implicit def javaHashSetConverter[T : TypeConverter] =
    new CollectionConverter[java.util.HashSet[T], T] { def collectionTypeString = "java.util.HashSet" }

  implicit def javaMapConverter[K : TypeConverter, V : TypeConverter] =
    new CollectionConverter[java.util.Map[K, V], (K, V)] { def collectionTypeString = "java.util.Map" }

  implicit def javaHashMapConverter[K : TypeConverter, V : TypeConverter] =
    new CollectionConverter[java.util.HashMap[K, V], (K, V)] { def collectionTypeString = "java.util.HashMap" }

  private val OptionSymbol = typeOf[Option[Any]].asInstanceOf[TypeRef].sym
  private val ListSymbol = typeOf[List[Any]].asInstanceOf[TypeRef].sym
  private val VectorSymbol = typeOf[Vector[Any]].asInstanceOf[TypeRef].sym
  private val SetSymbol = typeOf[Set[Any]].asInstanceOf[TypeRef].sym
  private val TreeSetSymbol = typeOf[TreeSet[Any]].asInstanceOf[TypeRef].sym
  private val SeqSymbol = typeOf[Seq[Any]].asInstanceOf[TypeRef].sym
  private val IndexedSeqSymbol = typeOf[IndexedSeq[Any]].asInstanceOf[TypeRef].sym
  private val IterableSymbol = typeOf[Iterable[Any]].asInstanceOf[TypeRef].sym
  private val MapSymbol = typeOf[Map[Any, Any]].asInstanceOf[TypeRef].sym
  private val TreeMapSymbol = typeOf[TreeMap[Any, Any]].asInstanceOf[TypeRef].sym

  private val JavaListSymbol = typeOf[java.util.List[Any]].asInstanceOf[TypeRef].sym
  private val JavaArrayListSymbol = typeOf[java.util.ArrayList[Any]].asInstanceOf[TypeRef].sym
  private val JavaSetSymbol = typeOf[java.util.Set[Any]].asInstanceOf[TypeRef].sym
  private val JavaHashSetSymbol = typeOf[java.util.HashSet[Any]].asInstanceOf[TypeRef].sym
  private val JavaMapSymbol = typeOf[java.util.Map[Any, Any]].asInstanceOf[TypeRef].sym
  private val JavaHashMapSymbol = typeOf[java.util.HashMap[Any, Any]].asInstanceOf[TypeRef].sym

  private def orderingFor(tpe: Type): Option[Ordering[_]] = {
    if      (tpe =:= typeOf[Boolean]) Some(implicitly[Ordering[Boolean]])
    else if (tpe =:= typeOf[Int]) Some(implicitly[Ordering[Int]])
    else if (tpe =:= typeOf[Long]) Some(implicitly[Ordering[Long]])
    else if (tpe =:= typeOf[Float]) Some(implicitly[Ordering[Float]])
    else if (tpe =:= typeOf[Double]) Some(implicitly[Ordering[Double]])
    else if (tpe =:= typeOf[String]) Some(implicitly[Ordering[String]])
    else if (tpe =:= typeOf[BigInt]) Some(implicitly[Ordering[BigInt]])
    else if (tpe =:= typeOf[BigDecimal]) Some(implicitly[Ordering[BigDecimal]])
    else if (tpe =:= typeOf[java.math.BigInteger]) Some(implicitly[Ordering[java.math.BigInteger]])
    else if (tpe =:= typeOf[java.math.BigDecimal]) Some(implicitly[Ordering[java.math.BigDecimal]])
    else if (tpe =:= typeOf[java.util.Date]) Some(implicitly[Ordering[java.util.Date]])
    else if (tpe =:= typeOf[java.sql.Date]) Some(Ordering.by((x: java.sql.Date) => x.getTime))
    else if (tpe =:= typeOf[org.joda.time.DateTime]) Some(Ordering.by((x: org.joda.time.DateTime) => x.toDate.getTime))
    else if (tpe =:= typeOf[UUID]) Some(implicitly[Ordering[UUID]])
    else None
  }

  /** Useful for getting converter based on a type received from Scala reflection */
  def forType(tpe: Type): TypeConverter[_] = {
    if      (tpe =:= typeOf[Any]) AnyConverter
    else if (tpe =:= typeOf[AnyRef]) AnyRefConverter
    else if (tpe =:= typeOf[Boolean]) BooleanConverter
    else if (tpe =:= typeOf[Int]) IntConverter
    else if (tpe =:= typeOf[Long]) LongConverter
    else if (tpe =:= typeOf[Float]) FloatConverter
    else if (tpe =:= typeOf[Double]) DoubleConverter
    else if (tpe =:= typeOf[String]) StringConverter
    else if (tpe =:= typeOf[BigInt]) BigIntConverter
    else if (tpe =:= typeOf[BigDecimal]) BigDecimalConverter
    else if (tpe =:= typeOf[java.math.BigInteger]) JavaBigIntegerConverter
    else if (tpe =:= typeOf[java.math.BigDecimal]) JavaBigDecimalConverter
    else if (tpe =:= typeOf[java.util.Date]) DateConverter
    else if (tpe =:= typeOf[java.sql.Date]) SqlDateConverter
    else if (tpe =:= typeOf[org.joda.time.DateTime]) JodaDateConverter
    else if (tpe =:= typeOf[InetAddress]) InetAddressConverter
    else if (tpe =:= typeOf[UUID]) UUIDConverter
    else if (tpe =:= typeOf[ByteBuffer]) ByteBufferConverter
    else if (tpe =:= typeOf[Array[Byte]]) ByteArrayConverter
    else tpe match {
      case TypeRef(_, symbol, List(arg)) =>
        val untypedItemConverter = forType(arg)
        type T = untypedItemConverter.targetType
        implicit val itemConverter = untypedItemConverter.asInstanceOf[TypeConverter[T]]
        implicit val ordering = orderingFor(arg).map(_.asInstanceOf[Ordering[T]]).orNull
        symbol match {
          case OptionSymbol => optionConverter[T]
          case ListSymbol => listConverter[T]
          case VectorSymbol => vectorConverter[T]
          case SetSymbol => setConverter[T]
          case TreeSetSymbol if ordering != null => treeSetConverter[T]
          case SeqSymbol => seqConverter[T]
          case IndexedSeqSymbol => indexedSeqConverter[T]
          case IterableSymbol => iterableConverter[T]
          case JavaListSymbol => javaListConverter[T]
          case JavaArrayListSymbol => javaArrayListConverter[T]
          case JavaSetSymbol => javaSetConverter[T]
          case JavaHashSetSymbol => javaHashSetConverter[T]
          case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
        }

      case TypeRef(_, symbol, List(k, v)) =>
        val untypedKeyConverter = forType(k)
        val untypedValueConverter = forType(v)
        type K = untypedKeyConverter.targetType
        type V = untypedValueConverter.targetType
        implicit val keyConverter = untypedKeyConverter.asInstanceOf[TypeConverter[K]]
        implicit val valueConverter = untypedValueConverter.asInstanceOf[TypeConverter[V]]
        implicit val ordering = orderingFor(k).map(_.asInstanceOf[Ordering[K]]).orNull
        symbol match {
          case MapSymbol => mapConverter[K, V]
          case TreeMapSymbol if ordering != null => treeMapConverter[K, V]
          case JavaMapSymbol => javaMapConverter[K, V]
          case JavaHashMapSymbol => javaHashMapConverter[K, V]
          case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
        }

      case _ => throw new IllegalArgumentException(s"Unsupported type: $tpe")
    }
  }

  /** Useful when implicit converters are not in scope, but a TypeTag is */
  def forType[T : TypeTag]: TypeConverter[T] =
    forType(implicitly[TypeTag[T]].tpe).asInstanceOf[TypeConverter[T]]
}