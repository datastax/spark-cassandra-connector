package com.datastax.spark.connector.types

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.datastax.spark.connector.util.SerializationUtil
import org.joda.time.DateTime
import org.junit.Assert._
import org.junit.Test

import scala.collection.immutable.{TreeMap, TreeSet}

class TypeConverterTest {

  @Test
  def testBoolean() {
    val c = TypeConverter.forType[Boolean]
    assertEquals(true, c.convert("true"))
    assertEquals(false, c.convert("false"))
    assertEquals(true, c.convert(1))
    assertEquals(false, c.convert(0))
  }

  @Test
  def testInt() {
    val c = TypeConverter.forType[Int]
    assertEquals(12345, c.convert("12345"))
    assertEquals(12345, c.convert(12345))
  }

  @Test
  def testLong() {
    val c = TypeConverter.forType[Long]
    assertEquals(12345L, c.convert("12345"))
    assertEquals(12345L, c.convert(12345))
    assertEquals(12345L, c.convert(12345L))
  }

  @Test
  def testFloat() {
    val c = TypeConverter.forType[Float]
    assertEquals(1.0f, c.convert("1.0"), 0.0001f)
    assertEquals(1.0f, c.convert(1.0f), 0.0001f)
  }

  @Test
  def testDouble() {
    val c = TypeConverter.forType[Double]
    assertEquals(1.0, c.convert("1.0"), 0.0001)
    assertEquals(1.0, c.convert(1.0), 0.0001)
  }

  @Test
  def testBigInt() {
    val c = TypeConverter.forType[BigInt]
    assertEquals(BigInt(12345), c.convert(12345))
    assertEquals(BigInt("123456789123456789123456789"), c.convert("123456789123456789123456789"))
  }

  @Test
  def testJavaBigInteger() {
    val c = TypeConverter.forType[BigInteger]
    assertEquals(new BigInteger("12345"), c.convert(12345))
    assertEquals(new BigInteger("123456789123456789123456789"), c.convert("123456789123456789123456789"))
  }

  @Test
  def testBigDecimal() {
    val c = TypeConverter.forType[BigDecimal]
    assertEquals(BigDecimal(12345.25), c.convert(12345.25))
    assertEquals(BigDecimal("123456789123456789.123456789"), c.convert("123456789123456789.123456789"))
  }

  @Test
  def testJavaBigDecimal() {
    val c = TypeConverter.forType[java.math.BigDecimal]
    assertEquals(new java.math.BigDecimal("12345.25"), c.convert(12345.25))
    assertEquals(new java.math.BigDecimal("123456789123456789.123456789"), c.convert("123456789123456789.123456789"))
  }

  @Test
  def testString() {
    val c = TypeConverter.forType[String]
    assertEquals("a string", c.convert("a string"))
  }

  @Test
  def testDate() {
    val c = TypeConverter.forType[Date]
    val dateStr = "2014-04-23 11:21:32+0100"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val date = dateFormat.parse(dateStr)
    assertEquals(date, c.convert(dateStr))
  }

  @Test
  def testJodaTime() {
    val c = TypeConverter.forType[DateTime]
    val dateStr = "2014-04-23 11:21:32+0100"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val date = new DateTime(dateFormat.parse(dateStr))
    assertEquals(date, c.convert(dateStr))
  }

  @Test
  def testInetAddress() {
    val c = TypeConverter.forType[InetAddress]
    assertEquals(InetAddress.getByName("127.0.0.1"), c.convert("127.0.0.1"))
  }

  @Test
  def testUUID() {
    val c = TypeConverter.forType[UUID]
    val uuidStr = "550e8400-e29b-41d4-a716-446655440000"
    assertEquals(UUID.fromString(uuidStr), c.convert(uuidStr))
  }

  @Test
  def testByteArray() {
    val c = TypeConverter.forType[Array[Byte]]
    val array = Array[Byte](1, 2, 3, 4)
    val buf = ByteBuffer.allocate(4)
    buf.put(array)
    buf.rewind()
    assertSame(array, c.convert(array))
    assertEquals(array.deep, c.convert(buf).deep)
  }

  @Test
  def testOption() {
    val c = TypeConverter.forType[Option[String]]
    assertEquals(None, c.convert(null))
    assertEquals(Some("not-null"), c.convert("not-null"))
  }

  @Test
  def testList() {
    val c = TypeConverter.forType[Vector[Option[Int]]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    arrayList.add(null)
    assertEquals(Vector(Some(1), Some(2), None), c.convert(arrayList))
  }

  @Test
  def testSet() {
    val c = TypeConverter.forType[Set[Int]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    assertEquals(Set(1, 2), c.convert(arrayList))
  }

  @Test
  def testTreeSet() {
    val c = TypeConverter.forType[TreeSet[Int]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("2")
    arrayList.add("1")
    arrayList.add("4")
    arrayList.add("3")
    assertEquals(TreeSet(1, 2, 3, 4), c.convert(arrayList))
  }

  @Test
  def testMap() {
    val c = TypeConverter.forType[Map[Int, Option[String]]]
    val map = new java.util.HashMap[String, String]()
    map.put("1", "a")
    map.put("2", "b")
    map.put("3", null)
    assertEquals(Map(1 -> Some("a"), 2 -> Some("b"), 3 -> None), c.convert(map))
  }

  @Test
  def testTreeMap() {
    val c = TypeConverter.forType[TreeMap[Int, Option[String]]]
    val map = new java.util.HashMap[String, String]()
    map.put("1", "a")
    map.put("2", "b")
    map.put("3", null)
    assertEquals(TreeMap(1 -> Some("a"), 2 -> Some("b"), 3 -> None), c.convert(map))
  }

  private def abstractTestJavaList(c: TypeConverter[_]) {
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    val outList = new java.util.ArrayList[Int]()
    outList.add(1)
    outList.add(2)
    assertEquals(outList, c.convert(arrayList))
  }

  @Test
  def testJavaList() {
    abstractTestJavaList(TypeConverter.forType[java.util.List[Int]])
  }

  @Test
  def testJavaArrayList() {
    abstractTestJavaList(TypeConverter.forType[java.util.ArrayList[Int]])
  }

  private def abstractTestJavaSet(c: TypeConverter[_]) {
    val c = TypeConverter.forType[java.util.HashSet[Int]]
    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")
    val outSet = new java.util.HashSet[Int]()
    outSet.add(1)
    outSet.add(2)
    assertEquals(outSet, c.convert(arrayList))
  }

  @Test
  def testJavaSet() {
    abstractTestJavaSet(TypeConverter.forType[java.util.Set[Int]])
  }

  @Test
  def testJavaHashSet() {
    abstractTestJavaSet(TypeConverter.forType[java.util.HashSet[Int]])
  }

  private def abstractTestJavaMap(c: TypeConverter[_]) {
    val map = new java.util.HashMap[String, String]()
    map.put("1", "a")
    map.put("2", "b")
    map.put("3", null)
    val outMap = new java.util.HashMap[Int, Option[String]]()
    outMap.put(1, Some("a"))
    outMap.put(2, Some("b"))
    outMap.put(3, None)

    assertEquals(outMap, c.convert(map))
  }

  @Test
  def testJavaMap() {
    abstractTestJavaMap(TypeConverter.forType[java.util.Map[Int, Option[String]]])
  }

  @Test
  def testJavaHashMap() {
    abstractTestJavaMap(TypeConverter.forType[java.util.HashMap[Int, Option[String]]])
  }

  @Test
  def testOptionToNullConverter() {
    val c = new TypeConverter.OptionToNullConverter(TypeConverter.IntConverter)
    assertEquals(1.asInstanceOf[AnyRef], c.convert(Some(1)))
    assertEquals(1.asInstanceOf[AnyRef], c.convert(1))
    assertEquals(1.asInstanceOf[AnyRef], c.convert(Some("1")))
    assertEquals(1.asInstanceOf[AnyRef], c.convert("1"))
    assertEquals(null, c.convert(None))
    assertEquals(null, c.convert(null))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnsupportedType() {
    TypeConverter.forType[TypeConverterTest]
  }

  @Test
  def testSerializeCollectionConverter() {
    val c1 = TypeConverter.forType[Vector[Int]]
    val c2 = SerializationUtil.serializeAndDeserialize(c1)

    val arrayList = new java.util.ArrayList[String]()
    arrayList.add("1")
    arrayList.add("2")

    val out = c2.convert(arrayList)
    assertEquals(Vector(1, 2), out)
  }

  type StringAlias = String

  @Test
  def testTypeAliases() {
    assertNotNull(TypeConverter.forType[StringAlias])
    assertNotNull(TypeConverter.forType[java.lang.String])
    assertNotNull(TypeConverter.forType[scala.Predef.String])
    assertNotNull(TypeConverter.forType[List[StringAlias]])
    assertNotNull(TypeConverter.forType[List[java.lang.String]])
    assertNotNull(TypeConverter.forType[TreeSet[StringAlias]])
    assertNotNull(TypeConverter.forType[TreeSet[java.lang.String]])
    assertNotNull(TypeConverter.forType[Map[StringAlias, StringAlias]])
    assertNotNull(TypeConverter.forType[TreeMap[StringAlias, StringAlias]])
  }
}
