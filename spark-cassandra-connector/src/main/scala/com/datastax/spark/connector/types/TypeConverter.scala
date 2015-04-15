package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Calendar, GregorianCalendar, UUID, Date}

import scala.collection.JavaConversions._
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.reflect.runtime.universe._

import org.joda.time.DateTime

import com.datastax.spark.connector.UDTValue.UDTValueConverter
import com.datastax.spark.connector.util.{ByteBufferUtil, Symbols}
import Symbols._

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

  /** Returns a function converting an object into `T`. */
  def convertPF: PartialFunction[Any, T]

  /** Converts and object or throws TypeConversionException if the object can't be converted. */
  def convert(obj: Any): T = {
    convertPF.applyOrElse(obj, (_: Any) =>
      if (obj != null)
        throw new TypeConversionException(s"Cannot convert object $obj of type ${obj.getClass} to $targetTypeName.")
      else
        throw new TypeConversionException(s"Cannot convert object $obj to $targetTypeName.")
    )
  }
}

/** Handles nullable types and converts any null to null. */
trait NullableTypeConverter[T <: AnyRef] extends TypeConverter[T] {
  override def convert(obj: Any): T =
    if (obj != null)
      super.convert(obj)
    else
      null.asInstanceOf[T]
}

/** Chains together several converters converting to the same type.
  * This way you can extend functionality of any converter to support new input types. */
class ChainedTypeConverter[T](converters: TypeConverter[T]*) extends TypeConverter[T] {
  def targetTypeTag = converters.head.targetTypeTag
  def convertPF = converters.map(_.convertPF).reduceLeft(_ orElse _)
}

/** Defines a set of converters and implicit functions used to look up an appropriate converter for
  * a desired type. Thanks to implicit method lookup, it is possible to implement a generic
  * method `CassandraRow#get`, which picks up the right converter basing solely on its type argument. */
object TypeConverter {

  private val AnyTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Any]]
  }

  implicit object AnyConverter extends TypeConverter[Any] {
    def targetTypeTag = AnyTypeTag
    def convertPF = {
      case obj => obj
    }
  }

  private val AnyRefTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[AnyRef]]
  }

  implicit object AnyRefConverter extends TypeConverter[AnyRef] {
    def targetTypeTag = AnyRefTypeTag
    def convertPF = {
      case obj => obj.asInstanceOf[AnyRef]
    }
  }

  private val BooleanTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Boolean]]
  }

  implicit object BooleanConverter extends TypeConverter[Boolean] {
    def targetTypeTag = BooleanTypeTag
    def convertPF = {
      case x: java.lang.Boolean => x
      case x: java.lang.Integer => x != 0
      case x: java.lang.Long => x != 0L
      case x: java.math.BigInteger => x != java.math.BigInteger.ZERO
      case x: String => x.toBoolean
    }
  }

  private val JavaBooleanTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Boolean]]
  }

  implicit object JavaBooleanConverter extends NullableTypeConverter[java.lang.Boolean] {
    def targetTypeTag = JavaBooleanTypeTag
    def convertPF = BooleanConverter.convertPF.andThen(_.asInstanceOf[java.lang.Boolean])
  }

  private val ByteTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Byte]]
  }

  implicit object ByteConverter extends TypeConverter[Byte] {
    def targetTypeTag = ByteTypeTag
    def convertPF = {
      case x: Number => x.byteValue
      case x: String => x.toByte
    }
  }

  private val JavaByteTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Byte]]
  }

  implicit object JavaByteConverter extends NullableTypeConverter[java.lang.Byte] {
    def targetTypeTag = JavaByteTypeTag
    def convertPF = ByteConverter.convertPF.andThen(_.asInstanceOf[java.lang.Byte])
  }

  private val ShortTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Short]]
  }

  implicit object ShortConverter extends TypeConverter[Short] {
    def targetTypeTag = ShortTypeTag
    def convertPF = {
      case x: Number => x.shortValue
      case x: String => x.toShort
    }
  }

  private val JavaShortTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Short]]
  }

  implicit object JavaShortConverter extends NullableTypeConverter[java.lang.Short] {
    def targetTypeTag = JavaShortTypeTag
    def convertPF = ShortConverter.convertPF.andThen(_.asInstanceOf[java.lang.Short])
  }

  private val IntTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Int]]
  }

  implicit object IntConverter extends TypeConverter[Int] {
    def targetTypeTag = IntTypeTag
    def convertPF = {
      case x: Number => x.intValue
      case x: String => x.toInt
    }
  }

  private val JavaIntTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Integer]]
  }

  implicit object JavaIntConverter extends NullableTypeConverter[java.lang.Integer] {
    def targetTypeTag = JavaIntTypeTag
    def convertPF = IntConverter.convertPF.andThen(_.asInstanceOf[java.lang.Integer])
  }

  private val LongTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Long]]
  }

  implicit object LongConverter extends TypeConverter[Long] {
    def targetTypeTag = LongTypeTag
    def convertPF = {
      case x: Number => x.longValue
      case x: Date => x.getTime
      case x: DateTime => x.toDate.getTime
      case x: Calendar => x.getTimeInMillis
      case x: String => x.toLong
    }
  }

  private val JavaLongTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Long]]
  }

  implicit object JavaLongConverter extends NullableTypeConverter[java.lang.Long] {
    def targetTypeTag = JavaLongTypeTag
    def convertPF = LongConverter.convertPF.andThen(_.asInstanceOf[java.lang.Long])
  }

  private val FloatTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Float]]
  }

  implicit object FloatConverter extends TypeConverter[Float] {
    def targetTypeTag = FloatTypeTag
    def convertPF = {
      case x: Number => x.floatValue
      case x: String => x.toFloat
    }
  }

  private val JavaFloatTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Float]]
  }

  implicit object JavaFloatConverter extends NullableTypeConverter[java.lang.Float] {
    def targetTypeTag = JavaFloatTypeTag
    def convertPF = FloatConverter.convertPF.andThen(_.asInstanceOf[java.lang.Float])
  }

  private val DoubleTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Double]]
  }

  implicit object DoubleConverter extends TypeConverter[Double] {
    def targetTypeTag = DoubleTypeTag
    def convertPF = {
      case x: Number => x.doubleValue
      case x: String => x.toDouble
    }
  }

  private val JavaDoubleTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.lang.Double]]
  }

  implicit object JavaDoubleConverter extends NullableTypeConverter[java.lang.Double] {
    def targetTypeTag = JavaDoubleTypeTag
    def convertPF = DoubleConverter.convertPF.andThen(_.asInstanceOf[java.lang.Double])
  }

  private val StringTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[String]]
  }

  implicit object StringConverter extends NullableTypeConverter[String] {
    def targetTypeTag = StringTypeTag
    def convertPF = {
      case x: Date => TimestampFormatter.format(x)
      case x: Array[Byte] => "0x" + x.map("%02x" format _).mkString
      case x: Map[_, _] => x.map(kv => convert(kv._1) + ": " + convert(kv._2)).mkString("{", ",", "}")
      case x: Set[_] => x.map(convert).mkString("{", ",", "}")
      case x: Seq[_] => x.map(convert).mkString("[", ",", "]")
      case x: Any  => x.toString
    }
  }

  private val ByteBufferTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[ByteBuffer]]
  }

  implicit object ByteBufferConverter extends NullableTypeConverter[ByteBuffer] {
    def targetTypeTag = ByteBufferTypeTag
    def convertPF = {
      case x: ByteBuffer => x
      case x: Array[Byte] => ByteBuffer.wrap(x)
    }
  }

  private val ByteArrayTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Array[Byte]]]
  }

  implicit object ByteArrayConverter extends NullableTypeConverter[Array[Byte]] {
    def targetTypeTag = ByteArrayTypeTag
    def convertPF = {
      case x: Array[Byte] => x
      case x: ByteBuffer => ByteBufferUtil.toArray(x)
    }
  }

  private val DateTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[Date]]
  }

  implicit object DateConverter extends NullableTypeConverter[Date] {
    def targetTypeTag = DateTypeTag
    def convertPF = {
      case x: Date => x
      case x: DateTime => x.toDate
      case x: Calendar => x.getTime
      case x: Long => new Date(x)
      case x: UUID if x.version() == 1 => new Date(x.timestamp())
      case x: String => TimestampParser.parse(x)
    }
  }

  private val SqlDateTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.sql.Date]]
  }

  implicit object SqlDateConverter extends NullableTypeConverter[java.sql.Date] {
    def targetTypeTag = SqlDateTypeTag
    def convertPF = DateConverter.convertPF.andThen(d => new java.sql.Date(d.getTime))
  }

  private val JodaDateTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[DateTime]]
  }

  implicit object JodaDateConverter extends NullableTypeConverter[DateTime] {
    def targetTypeTag = JodaDateTypeTag
    def convertPF = DateConverter.convertPF.andThen(new DateTime(_))
  }

  private val GregorianCalendarTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[GregorianCalendar]]
  }

  implicit object GregorianCalendarConverter extends NullableTypeConverter[GregorianCalendar] {
    private[this] def calendar(date: Date): GregorianCalendar = {
      val c = new GregorianCalendar()
      c.setTime(date)
      c
    }
    def targetTypeTag = GregorianCalendarTypeTag
    def convertPF = DateConverter.convertPF.andThen(calendar)
  }

  private val BigIntTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[BigInt]]
  }

  implicit object BigIntConverter extends NullableTypeConverter[BigInt] {
    def targetTypeTag = BigIntTypeTag
    def convertPF = {
      case x: BigInt => x
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => BigInt(x)
      case x: java.lang.Long => BigInt(x)
      case x: String => BigInt(x)
    }
  }

  private val JavaBigIntegerTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.math.BigInteger]]
  }

  implicit object JavaBigIntegerConverter extends NullableTypeConverter[java.math.BigInteger] {
    def targetTypeTag = JavaBigIntegerTypeTag
    def convertPF = {
      case x: BigInt => x.bigInteger
      case x: java.math.BigInteger => x
      case x: java.lang.Integer => new java.math.BigInteger(x.toString)
      case x: java.lang.Long => new java.math.BigInteger(x.toString)
      case x: String => new java.math.BigInteger(x)
    }
  }

  private val BigDecimalTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[BigDecimal]]
  }

  implicit object BigDecimalConverter extends NullableTypeConverter[BigDecimal] {
    def targetTypeTag = BigDecimalTypeTag
    def convertPF = {
      case x: Number => BigDecimal(x.toString)
      case x: String => BigDecimal(x)
    }
  }

  private val JavaBigDecimalTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[java.math.BigDecimal]]
  }

  implicit object JavaBigDecimalConverter extends NullableTypeConverter[java.math.BigDecimal] {
    def targetTypeTag = JavaBigDecimalTypeTag
    def convertPF = {
      case x: Number => new java.math.BigDecimal(x.toString)
      case x: String => new java.math.BigDecimal(x)
    }
  }

  private val UUIDTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[UUID]]
  }

  implicit object UUIDConverter extends NullableTypeConverter[UUID] {
    def targetTypeTag = UUIDTypeTag
    def convertPF = {
      case x: UUID => x
      case x: String => UUID.fromString(x)
    }
  }

  private val InetAddressTypeTag = TypeTag.synchronized {
    implicitly[TypeTag[InetAddress]]
  }

  implicit object InetAddressConverter extends NullableTypeConverter[InetAddress] {
    def targetTypeTag = InetAddressTypeTag
    def convertPF = {
      case x: InetAddress => x
      case x: String => InetAddress.getByName(x)
    }
  }

  class TupleConverter[K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V])
    extends TypeConverter[(K, V)] {

    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicit val kTag = kc.targetTypeTag
      implicit val vTag = vc.targetTypeTag
      implicitly[TypeTag[(K, V)]]
    }
    
    def convertPF = {
      case (k, v) => (kc.convert(k), vc.convert(v))
    }
  }

  class OptionConverter[T](implicit c: TypeConverter[T]) extends TypeConverter[Option[T]] {

    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicit val itemTypeTag = c.targetTypeTag
      implicitly[TypeTag[Option[T]]]
    }

    def convertPF = {
      case null => None
      case other => Some(c.convert(other))
    }
  }

  abstract class CollectionConverter[CC, T](implicit c: TypeConverter[T], bf: CanBuildFrom[T, CC])
    extends TypeConverter[CC] {

    protected implicit def itemTypeTag: TypeTag[T] = c.targetTypeTag

    private def newCollection(items: Iterable[Any]) = {
      val builder = bf()
      for (item <- items)
        builder += c.convert(item)
      builder.result()
    }

    def convertPF = {
      case null => bf.apply().result()
      case x: java.util.List[_] => newCollection(x)
      case x: java.util.Set[_] => newCollection(x)
      case x: java.util.Map[_, _] => newCollection(x)
      case x: Iterable[_] => newCollection(x)
    }
  }

  abstract class AbstractMapConverter[CC, K, V](implicit kc: TypeConverter[K], vc: TypeConverter[V], bf: CanBuildFrom[(K, V), CC])
    extends CollectionConverter[CC, (K, V)] {

    protected implicit def keyTypeTag: TypeTag[K] = kc.targetTypeTag
    protected implicit def valueTypeTag: TypeTag[V] = vc.targetTypeTag
  }


  class ListConverter[T : TypeConverter] extends CollectionConverter[List[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[List[T]]]
    }
  }

  class VectorConverter[T : TypeConverter] extends CollectionConverter[Vector[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Vector[T]]]
    }
  }

  class SetConverter[T : TypeConverter] extends CollectionConverter[Set[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Set[T]]]
    }
  }

  class TreeSetConverter[T : TypeConverter : Ordering] extends CollectionConverter[TreeSet[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[TreeSet[T]]]
    }
  }

  class SeqConverter[T : TypeConverter] extends CollectionConverter[Seq[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Seq[T]]]
    }
  }

  class IndexedSeqConverter[T : TypeConverter] extends CollectionConverter[IndexedSeq[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[IndexedSeq[T]]]
    }
  }

  class IterableConverter[T : TypeConverter] extends CollectionConverter[Iterable[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Iterable[T]]]
    }
  }

  class JavaListConverter[T : TypeConverter] extends CollectionConverter[java.util.List[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[java.util.List[T]]]
    }
  }

  class JavaArrayListConverter[T : TypeConverter] extends CollectionConverter[java.util.ArrayList[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[java.util.ArrayList[T]]]
    }
  }

  class JavaSetConverter[T : TypeConverter] extends CollectionConverter[java.util.Set[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[java.util.Set[T]]]
    }
  }

  class JavaHashSetConverter[T : TypeConverter] extends CollectionConverter[java.util.HashSet[T], T] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[java.util.HashSet[T]]]
    }
  }

  class MapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[Map[K, V], K, V] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Map[K, V]]]
    }
  }

  class TreeMapConverter[K : TypeConverter : Ordering, V : TypeConverter] extends AbstractMapConverter[TreeMap[K, V], K, V] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[TreeMap[K, V]]]
    }
  }

  class JavaMapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[java.util.Map[K, V], K, V] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[java.util.Map[K, V]]]
    }
  }

  class JavaHashMapConverter[K : TypeConverter, V : TypeConverter] extends AbstractMapConverter[java.util.HashMap[K, V], K, V] {
    @transient
    lazy val targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[java.util.HashMap[K, V]]]
    }
  }

  implicit def optionConverter[T : TypeConverter]: OptionConverter[T] =
    new OptionConverter[T]

  implicit def tupleConverter[K : TypeConverter, V : TypeConverter]: TupleConverter[K, V] =
    new TupleConverter[K, V]

  implicit def listConverter[T : TypeConverter]: ListConverter[T] =
    new ListConverter[T]

  implicit def vectorConverter[T : TypeConverter]: VectorConverter[T] =
    new VectorConverter[T]

  implicit def setConverter[T : TypeConverter]: SetConverter[T] =
    new SetConverter[T]

  implicit def treeSetConverter[T : TypeConverter : Ordering]: TreeSetConverter[T] =
    new TreeSetConverter[T]

  implicit def seqConverter[T : TypeConverter]: SeqConverter[T] =
    new SeqConverter[T]

  implicit def indexedSeqConverter[T : TypeConverter]: IndexedSeqConverter[T] =
    new IndexedSeqConverter[T]

  implicit def iterableConverter[T : TypeConverter]: IterableConverter[T] =
    new IterableConverter[T]

  implicit def mapConverter[K : TypeConverter, V : TypeConverter]: MapConverter[K, V] =
    new MapConverter[K, V]

  implicit def treeMapConverter[K: TypeConverter : Ordering, V : TypeConverter]: TreeMapConverter[K, V] =
    new TreeMapConverter[K, V]

  // Support for Java collections:
  implicit def javaListConverter[T : TypeConverter]: JavaListConverter[T] =
    new JavaListConverter[T]

  implicit def javaArrayListConverter[T : TypeConverter]: JavaArrayListConverter[T] =
    new JavaArrayListConverter[T]

  implicit def javaSetConverter[T : TypeConverter]: JavaSetConverter[T] =
    new JavaSetConverter[T]

  implicit def javaHashSetConverter[T : TypeConverter]: JavaHashSetConverter[T] =
    new JavaHashSetConverter[T]

  implicit def javaMapConverter[K : TypeConverter, V : TypeConverter]: JavaMapConverter[K, V] =
    new JavaMapConverter[K, V]

  implicit def javaHashMapConverter[K : TypeConverter, V : TypeConverter]: JavaHashMapConverter[K, V] =
    new JavaHashMapConverter[K, V]

  /** Converts Scala Options to Java nullable references. Used when saving data to Cassandra. */
  class OptionToNullConverter(nestedConverter: TypeConverter[_]) extends NullableTypeConverter[AnyRef] {

    def targetTypeTag = implicitly[TypeTag[AnyRef]]

    def convertPF = {
      case Some(x) => nestedConverter.convert(x).asInstanceOf[AnyRef]
      case None => null
      case x => nestedConverter.convert(x).asInstanceOf[AnyRef]
    }
  }

  private def orderingFor(tpe: Type): Option[Ordering[_]] = {
    if      (tpe =:= typeOf[Boolean]) Some(implicitly[Ordering[Boolean]])
    else if (tpe =:= typeOf[Byte]) Some(implicitly[Ordering[Byte]])
    else if (tpe =:= typeOf[Short]) Some(implicitly[Ordering[Short]])
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

  private var converters = Seq[TypeConverter[_]](
    AnyConverter,
    AnyRefConverter,
    BooleanConverter,
    JavaBooleanConverter,
    ByteConverter,
    JavaByteConverter,
    ShortConverter,
    JavaShortConverter,
    IntConverter,
    JavaIntConverter,
    LongConverter,
    JavaLongConverter,
    FloatConverter,
    JavaFloatConverter,
    DoubleConverter,
    JavaDoubleConverter,
    StringConverter,
    BigIntConverter,
    BigDecimalConverter,
    JavaBigIntegerConverter,
    JavaBigDecimalConverter,
    DateConverter,
    SqlDateConverter,
    JodaDateConverter,
    GregorianCalendarConverter,
    InetAddressConverter,
    UUIDConverter,
    ByteBufferConverter,
    ByteArrayConverter,
    UDTValueConverter
  )

  private def forCollectionType(tpe: Type): TypeConverter[_] = TypeTag.synchronized {
    tpe match {
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

  /** Useful for getting converter based on a type received from Scala reflection.
    * Synchronized to workaround Scala 2.10 reflection thread-safety problems. */
  def forType(tpe: Type): TypeConverter[_] = TypeTag.synchronized {
    type T = TypeConverter[_]
    val selectedConverters =
      converters.collect { case c: T if c.targetTypeTag.tpe =:= tpe => c }

    selectedConverters match {
      case Seq() => forCollectionType(tpe)
      case Seq(c) => c
      case Seq(cs @ _*) => new ChainedTypeConverter(cs : _*)
   }
  }

  /** Useful when implicit converters are not in scope, but a TypeTag is.
    * Synchronized to workaround Scala 2.10 reflection thread-safety problems. */
  def forType[T : TypeTag]: TypeConverter[T] = TypeTag.synchronized {
    forType(implicitly[TypeTag[T]].tpe).asInstanceOf[TypeConverter[T]]
  }

  /** Registers a custom converter */
  def registerConverter(c: TypeConverter[_]) {
    synchronized {
      converters = c +: converters
    }
  }
}