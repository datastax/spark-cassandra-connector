package com.datastax.spark.connector.rdd.typeTests

import java.nio.ByteBuffer

import com.datastax.driver.core.utils.Bytes
import com.datastax.spark.connector.CassandraRow

import scala.collection.JavaConverters._

class BlobTypeTest extends AbstractTypeTest[Array[Byte], ByteBuffer] {

  private def getByteArray(x: String): Array[Byte] = {
    Bytes.getArray(Bytes.fromHexString(x))
  }

  override val typeName = "blob"
  override val typeData: Seq[Array[Byte]] = Seq(getByteArray("0xFFFFFFFF"), getByteArray("0xEEEEEEEE"), getByteArray("0xDDDDDDDD"), getByteArray("0xCCCCCCCC"), getByteArray("0xBBBBBBBB"))
  override val typeSet: Set[Array[Byte]] = Set(getByteArray("0x01010101"), getByteArray("0x02020202"), getByteArray("0x03030303"))
  override val typeMap1: Map[String, Array[Byte]] = Map("key1" -> getByteArray("0xA1A1A1A1"), "key2" -> getByteArray("0xA1A1A1A1"), "key3" -> getByteArray("0xA1A1A1A1"))
  override val typeMap2: Map[Array[Byte], String] = Map(getByteArray("0x1C1C1C1C") -> "val1", getByteArray("0x1B1B1B1B") -> "val2", getByteArray("0x1A1A1A1A") -> "val3")

  override val addData: Seq[Array[Byte]] = Seq(getByteArray("0x00000000"), getByteArray("0x11111111"), getByteArray("0x22222222"), getByteArray("0x33333333"), getByteArray("0x44444444"))
  override val addSet: Set[Array[Byte]] = Set(getByteArray("0x00000001"), getByteArray("0x00000002"), getByteArray("0x00000003"))
  override val addMap1: Map[String, Array[Byte]] = Map("key4" -> getByteArray("0x10000000"), "key5" -> getByteArray("0x20000000"), "key3" -> getByteArray("0x30000000"))
  override val addMap2: Map[Array[Byte], String] = Map(getByteArray("0x60000000") -> "val4", getByteArray("0x50000000") -> "val5", getByteArray("0x40000000") -> "val6")

  private val bufferTypeData: Seq[ByteBuffer] = Seq(Bytes.fromHexString("0xFFFFFFFF"), Bytes.fromHexString("0xEEEEEEEE"), Bytes.fromHexString("0xDDDDDDDD"), Bytes.fromHexString("0xCCCCCCCC"), Bytes.fromHexString("0xBBBBBBBB"))
  private val bufferTypeSet: Set[ByteBuffer] = Set(Bytes.fromHexString("0x01010101"), Bytes.fromHexString("0x02020202"), Bytes.fromHexString("0x03030303"))
  private val bufferTypeMap1: Map[String,ByteBuffer] = Map("key1" -> Bytes.fromHexString("0xA1A1A1A1"), "key2" -> Bytes.fromHexString("0xA1A1A1A1"), "key3" -> Bytes.fromHexString("0xA1A1A1A1"))
  private val bufferTypeMap2: Map[ByteBuffer,String] = Map(Bytes.fromHexString("0x1C1C1C1C") -> "val1", Bytes.fromHexString("0x1B1B1B1B") -> "val2", Bytes.fromHexString("0x1A1A1A1A") -> "val3")

  override def convertToDriverInsertable(testValue: Array[Byte]): ByteBuffer = ByteBuffer.wrap(testValue)

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Array[Byte] = {
    row.getBytes(colName).array()
  }

  /*We need to override checks because of the Array not working with contains, so instead we convert
  everything to Strings where we can compare them without issue*/


  override def checkJDriverInsertsCollections() = conn.withSessionDo { session =>
    var resultSet = session.execute(s"SELECT * FROM ${typeName}_set")
    var rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    rows.foreach { row =>
      val resSet = row.getSet("data1", classOf[ByteBuffer])
      resSet.size() should equal(bufferTypeSet.size)
      resSet should contain theSameElementsAs bufferTypeSet
    }

    resultSet = session.execute(s"SELECT * FROM ${typeName}_list")
    rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    rows.foreach { row =>
      val resSet = row.getList("data1", classOf[ByteBuffer])
      resSet.size() should equal(bufferTypeSet.size)
      resSet should contain theSameElementsAs bufferTypeSet
    }

    resultSet = session.execute(s"SELECT * FROM ${typeName}_map_1")
    rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    rows.foreach { row =>
      val resMap = row.getMap("data1", classOf[String], classOf[ByteBuffer]).asScala
        .mapValues(Bytes.toHexString(_))
      resMap should contain theSameElementsAs bufferTypeMap1.mapValues( Bytes.toHexString(_))
    }

    resultSet = session.execute(s"SELECT * FROM ${typeName}_map_2")
    rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    rows.foreach { row =>
      val resMap = row.getMap("data1", classOf[ByteBuffer], classOf[String]).asScala
        .map{ case (k,v) => (Bytes.toHexString(k),v)}
      resMap should contain theSameElementsAs bufferTypeMap2.map{ case (k,v) => (Bytes.toHexString(k),v)}
    }
  }

  override def checkNormalRowConsistency(expectedKeys: Seq[Array[Byte]], results: Seq[(Array[Byte],Array[Byte],Array[Byte])]) {
    val expectedBytesAsHex = expectedKeys.map(Bytes.toHexString(_))
    val resultBytesAsHex = results.map { case (pkey, ckey, data) =>
      (Bytes.toHexString(pkey), Bytes.toHexString(ckey), Bytes.toHexString(data))
    }
    val foundBytesAsHex = resultBytesAsHex.filter( x => expectedBytesAsHex.contains(x._1))
    foundBytesAsHex should contain theSameElementsAs expectedBytesAsHex.map(x => (x,x,x))
  }

  override def checkCollectionConsistency(
                                  expectedKeys: Seq[Array[Byte]],
                                  expectedSet: Set[Array[Byte]],
                                  results: Seq[(Array[Byte],Set[Array[Byte]])]) {

    val expectedBytesAsHex = expectedKeys.map(Bytes.toHexString(_))
    val resultBytesAsHex = results.map { case (pkey, data) =>
      (Bytes.toHexString(pkey), data.map(Bytes.toHexString(_)))
    }
    val foundBytesAsHex = resultBytesAsHex.filter( x => expectedBytesAsHex.contains(x._1))
    foundBytesAsHex should contain theSameElementsAs
      expectedBytesAsHex.map(x => (x,expectedSet.map(Bytes.toHexString(_))))
  }

  override def checkMap1Consistenecy(
    expectedKeys: Seq[Array[Byte]],
    expectedMap: Map[String,Array[Byte]],
    results: Seq[(Array[Byte],
    Map[String,Array[Byte]])]): Unit = {

    val expectedBytesAsHex = expectedKeys.map(Bytes.toHexString(_))
    val resultBytesAsHex = results.map { case (pkey, data) =>
      (Bytes.toHexString(pkey), data.mapValues(Bytes.toHexString(_)))}
    val foundBytesAsHex = resultBytesAsHex.filter( x => expectedBytesAsHex.contains(x._1))
    foundBytesAsHex should contain theSameElementsAs
      expectedBytesAsHex.map(x => (x, expectedMap.mapValues(Bytes.toHexString(_))))
  }

  override def checkMap2Consistenecy(
                             expectedKeys: Seq[Array[Byte]],
                             expectedMap: Map[Array[Byte], String],
                             results: Seq[(Array[Byte], Map[Array[Byte], String])]) {

    val expectedBytesAsHex = expectedKeys.map(Bytes.toHexString(_))
    val resultBytesAsHex = results.map { case (pkey, data) =>
      (Bytes.toHexString(pkey), data.map{ case (k,v) => (Bytes.toHexString(k), v)})}
    val foundBytesAsHex = resultBytesAsHex.filter( x => expectedBytesAsHex.contains(x._1))
    foundBytesAsHex should contain theSameElementsAs
      expectedBytesAsHex.map(x => (x, expectedMap.map{ case (k,v) => (Bytes.toHexString(k), v)}))
  }
}

