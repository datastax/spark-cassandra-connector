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

  /** TypeTag for the target type. */
  def targetTypeTag: TypeTag[T]

  /** String representation of the converter target type.*/
  def targetTypeName: String =
    targetTypeTag.tpe.toString

  /** Converts an object into `T`. */
  def convert(obj: Any): T
}

/** Defines a set of converters and implicit functions used to look up an appropriate converter for
  * a desired type. Thanks to implicit method lookup, it is possible to implement a generic
  * method `CassandraRow#get`, which picks up the right converter basing solely on its type argument. */
object TypeConverter {

  implicit object AnyConverter extends TypeConverter[Any] {
    def targetTypeTag = implicitly[TypeTag[Any]]
    def convert(obj: Any) = obj
  }

  implicit object AnyRefConverter extends TypeConverter[AnyRef] {
    def targetTypeTag = implicitly[TypeTag[AnyRef]]
    def convert(obj: Any) = obj.asInstanceOf[AnyRef]
  }

  implicit object BooleanConverter extends TypeConverter[Boolean] {
    def targetTypeTag = implicitly[TypeTag[Boolean]]
    def convert(obj: Any) = obj match {
      case x: java.lang.Boolean => x
      case x: java.lang.Integer => x != 0
      case x: java.lang.Long => x != 0L
      case x: java.math.BigInteger => x != java.math.BigInteger.ZERO
      case x: String => x.toBoolean
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object IntConverter extends TypeConverter[Int] {
    def targetTypeTag = implicitly[TypeTag[Int]]
    def convert(obj: Any) = obj match {
      case x: Number => x.intValue
      case x: String => x.toInt
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object LongConverter extends TypeConverter[Long] {
    def targetTypeTag = implicitly[TypeTag[Long]]
    def convert(obj: Any) = obj match {
      case x: Number => x.longValue
      case x: Date => x.getTime
      case x: String => x.toLong
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object FloatConverter extends TypeConverter[Float] {
    def targetTypeTag = implicitly[TypeTag[Float]]
    def convert(obj: Any) = obj match {
      case x: Number => x.floatValue
      case x: String => x.toFloat
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object DoubleConverter extends TypeConverter[Double] {
    def targetTypeTag = implicitly[TypeTag[Double]]
    def convert(obj: Any) = obj match {
      case x: Number => x.doubleValue
      case x: String => x.toDouble
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object StringConverter extends TypeConverter[String] {
    def targetTypeTag = implicitly[TypeTag[String]]
    def convert(obj: Any) = obj match {
      case x: Date => TimestampFormatter.format(x)
      case x: Array[Byte] => "0x" + x.map("%02x" format _).mkString
      case x: Map[_, _] => x.map(kv => convert(kv._1) + ": " + convert(kv._2)).mkString("{", ",", "}")
      case x: Set[_] => x.map(convert).mkString("{", ",", "}")
      case x: Seq[_] => x.map(convert).mkString("[", ",", "]")
      case x => x.toString
    }
  }

  implicit object ByteBufferConverter extends TypeConverter[ByteBuffer] {
    def targetTypeTag = implicitly[TypeTag[ByteBuffer]]
    def convert(obj: Any) = obj match {
      case x: ByteBuffer => x
      case x: Array[Byte] => ByteBuffer.wrap(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object ByteArrayConverter extends TypeConverter[Array[Byte]] {
    def targetTypeTag = implicitly[TypeTag[Array[Byte]]]
    def convert(obj: Any) = obj match {
      case x: Array[Byte] => x
      case x: ByteBuffer => ByteBufferUtil.getArray(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object DateConverter extends TypeConverter[Date] {
    def targetTypeTag = implicitly[TypeTag[Date]]
    def convert(obj: Any) = obj match {
      case x: Date => x
      case x: DateTime => x.toDate
      case x: Long => new Date(x)
      case x: UUID if x.version() == 1 => new Date(x.timestamp())
      case x: String => TimestampParser.parse(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }

  }

  implicit object SqlDateConverter extends TypeConverter[java.sql.Date] {
    def targetTypeTag = implicitly[TypeTag[java.sql.Date]]
    def convert(obj: Any) = obj match {
      case x: java.sql.Date => x
      case x: Date => new java.sql.Date(x.getTime)
      case x: DateTime => new java.sql.Date(x.toDate.getTime)
      case x: Long => new java.sql.Date(x)
      case x: UUID if x.version() == 1 => new java.sql.Date(x.timestamp())
      case x: String => new java.sql.Date(TimestampParser.parse(x).getTime)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object JodaDateConverter extends TypeConverter[DateTime] {
    def targetTypeTag = implicitly[TypeTag[DateTime]]
    def convert(obj: Any) = obj match {
      case x: DateTime => x
      case x: Date => new DateTime(x)
      case x: Long => new DateTime(x)
      case x: UUID if x.version() == 1 => new DateTime(x.timestamp())
      case x: String => new DateTime(TimestampParser.parse(x))
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object BigIntConverter extends TypeConverter[BigInt] {
    def targetTypeTag = implicitly[TypeTag[BigInt]]
    def convert(obj: Any) = obj match {
      case x: BigInt => x
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => BigInt(x)
      case x: java.lang.Long => BigInt(x)
      case x: String => BigInt(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object JavaBigIntegerConverter extends TypeConverter[java.math.BigInteger] {
    def targetTypeTag = implicitly[TypeTag[java.math.BigInteger]]
    def convert(obj: Any) = obj match {
      case x: BigInt => x.bigInteger
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => new java.math.BigInteger(x.toString)
      case x: java.lang.Long => new java.math.BigInteger(x.toString)
      case x: String => new java.math.BigInteger(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object BigDecimalConverter extends TypeConverter[BigDecimal] {
    def targetTypeTag = implicitly[TypeTag[BigDecimal]]
    override def convert(obj: Any) = obj match {
      case x: Number => BigDecimal(x.toString)
      case x: String => BigDecimal(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object JavaBigDecimalConverter extends TypeConverter[java.math.BigDecimal] {
    def targetTypeTag = implicitly[TypeTag[java.math.BigDecimal]]
    def convert(obj: Any) = obj match {
      case x: Number => new java.math.BigDecimal(x.toString)
      case x: String => new java.math.BigDecimal(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object UUIDConverter extends TypeConverter[UUID] {
    def targetTypeTag = implicitly[TypeTag[UUID]]
    override def convert(obj: Any) = obj match {
      case x: UUID => x
      case x: String => UUID.fromString(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  implicit object InetAddressConverter extends TypeConverter[InetAddress] {
    def targetTypeTag = implicitly[TypeTag[InetAddress]]
    override def convert(obj: Any) = obj match {
      case x: InetAddress => x
      case x: String => InetAddress.getByName(x)
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  class TupleConverter[K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V])
    extends TypeConverter[(K, V)] {

    @transient
    lazy val targetTypeTag = {
      implicit val kTag = kc.targetTypeTag
      implicit val vTag = vc.targetTypeTag
      implicitly[TypeTag[(K, V)]]
    }
    
    override def convert(obj: Any) = obj match {
      case (k, v) => (kc.convert(k), vc.convert(v))
      case x => throw new TypeConversionException(s"Cannot convert object $x to $targetTypeName.")
    }
  }

  class OptionConverter[T](implicit c: TypeConverter[T]) extends TypeConverter[Option[T]] {
    def targetTypeTag = {
      implicit val itemTypeTag = c.targetTypeTag
      implicitly[TypeTag[Option[T]]]
    }
    override def convert(obj: Any) = obj match {
      case null => None
      case other => Some(c.convert(obj))
    }
  }

  abstract class CollectionConverter[CC, T](implicit c: TypeConverter[T], bf: CanBuildFrom[T, CC])
    extends TypeConverter[CC] {

    protected implicit def itemTypeTag = c.targetTypeTag

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
      case x => throw new TypeConversionException(s"Cannot convert $x to $targetTypeName.")
    }
  }

  abstract class AbstractMapConverter[CC, K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V], bf: CanBuildFrom[(K, V), CC])
    extends CollectionConverter[CC, (K, V)] {

    protected implicit def keyTypeTag = kc.targetTypeTag
    protected implicit def valueTypeTag = vc.targetTypeTag
  }


  class ListConverter[T : TypeConverter] extends CollectionConverter[List[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[List[T]]]
  }

  class VectorConverter[T : TypeConverter] extends CollectionConverter[Vector[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[Vector[T]]]
  }

  class SetConverter[T : TypeConverter] extends CollectionConverter[Set[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[Set[T]]]
  }

  class TreeSetConverter[T : TypeConverter : Ordering] extends CollectionConverter[TreeSet[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[TreeSet[T]]]
  }

  class SeqConverter[T : TypeConverter] extends CollectionConverter[Seq[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[Seq[T]]]
  }

  class IndexedSeqConverter[T : TypeConverter] extends CollectionConverter[IndexedSeq[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[IndexedSeq[T]]]
  }

  class IterableConverter[T : TypeConverter] extends CollectionConverter[Iterable[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[Iterable[T]]]
  }

  class JavaListConverter[T : TypeConverter] extends CollectionConverter[java.util.List[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[java.util.List[T]]]
  }

  class JavaArrayListConverter[T : TypeConverter] extends CollectionConverter[java.util.ArrayList[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[java.util.ArrayList[T]]]
  }

  class JavaSetConverter[T : TypeConverter] extends CollectionConverter[java.util.Set[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[java.util.Set[T]]]
  }

  class JavaHashSetConverter[T : TypeConverter] extends CollectionConverter[java.util.HashSet[T], T] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[java.util.HashSet[T]]]
  }

  class MapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[Map[K, V], K, V] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[Map[K, V]]]
  }

  class TreeMapConverter[K : TypeConverter : Ordering, V : TypeConverter] extends AbstractMapConverter[TreeMap[K, V], K, V] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[TreeMap[K, V]]]
  }

  class JavaMapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[java.util.Map[K, V], K, V] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[java.util.Map[K, V]]]
  }

  class JavaHashMapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[java.util.HashMap[K, V], K, V] {
    @transient
    lazy val targetTypeTag = implicitly[TypeTag[java.util.HashMap[K, V]]]
  }

  implicit def optionConverter[T : TypeConverter] =
    new OptionConverter[T]

  implicit def tupleConverter[K : TypeConverter, V : TypeConverter] =
    new TupleConverter[K, V]

  implicit def listConverter[T : TypeConverter] =
    new ListConverter[T]

  implicit def vectorConverter[T : TypeConverter] =
    new VectorConverter[T]

  implicit def setConverter[T : TypeConverter] =
    new SetConverter[T]

  implicit def treeSetConverter[T : TypeConverter : Ordering] =
    new TreeSetConverter[T]

  implicit def seqConverter[T : TypeConverter] =
    new SeqConverter[T]

  implicit def indexedSeqConverter[T : TypeConverter] =
    new IndexedSeqConverter[T]

  implicit def iterableConverter[T : TypeConverter] =
    new IterableConverter[T]

  implicit def mapConverter[K : TypeConverter, V : TypeConverter] =
    new MapConverter[K, V]

  implicit def treeMapConverter[K: TypeConverter : Ordering, V : TypeConverter] =
    new TreeMapConverter[K, V]

  // Support for Java collections:
  implicit def javaListConverter[T : TypeConverter] =
    new JavaListConverter[T]

  implicit def javaArrayListConverter[T : TypeConverter] =
    new JavaArrayListConverter[T]

  implicit def javaSetConverter[T : TypeConverter] =
    new JavaSetConverter[T]

  implicit def javaHashSetConverter[T : TypeConverter] =
    new JavaHashSetConverter[T]

  implicit def javaMapConverter[K : TypeConverter, V : TypeConverter] =
    new JavaMapConverter[K, V]

  implicit def javaHashMapConverter[K : TypeConverter, V : TypeConverter] =
    new JavaHashMapConverter[K, V]

  /** Converts Scala Options to Java nullable references. Used when saving data to Cassandra. */
  class OptionToNullConverter(nestedConverter: TypeConverter[_]) extends TypeConverter[AnyRef] {

    def targetTypeTag = implicitly[TypeTag[AnyRef]]

    def convert(obj: Any) = obj match {
      case Some(x) => nestedConverter.convert(x).asInstanceOf[AnyRef]
      case None => null
      case null => null
      case x => nestedConverter.convert(x).asInstanceOf[AnyRef]
    }
  }

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
    else if (tpe =:= typeOf[java.lang.Boolean]) BooleanConverter
    else if (tpe =:= typeOf[Int]) IntConverter
    else if (tpe =:= typeOf[java.lang.Integer]) IntConverter
    else if (tpe =:= typeOf[Long]) LongConverter
    else if (tpe =:= typeOf[java.lang.Long]) LongConverter
    else if (tpe =:= typeOf[Float]) FloatConverter
    else if (tpe =:= typeOf[java.lang.Float]) FloatConverter
    else if (tpe =:= typeOf[Double]) DoubleConverter
    else if (tpe =:= typeOf[java.lang.Double]) DoubleConverter
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