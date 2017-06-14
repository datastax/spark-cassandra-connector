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
  override val typeData: Seq[Array[Byte]] = Seq(getByteArray("0xFFFFFFFF"), getByteArray("0xEEEEEEEE"))
  override val addData: Seq[Array[Byte]] = Seq(getByteArray("0x00000000"), getByteArray("0x11111111"))

  private val bufferTypeData: Seq[ByteBuffer] = typeData.map(ByteBuffer.wrap(_))
  private val bufferTypeSet: Set[ByteBuffer] = bufferTypeData.toSet
  private val bufferTypeMap1: Map[String,ByteBuffer] = bufferTypeData
    .zipWithIndex.map( pair => (s"Element ${pair._2}", pair._1)).toMap
  private val bufferTypeMap2: Map[ByteBuffer,String] = bufferTypeMap1.map(_.swap)

  override def convertToDriverInsertable(testValue: Array[Byte]): ByteBuffer = ByteBuffer.wrap(testValue)

  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): Array[Byte] = {
    row.getBytes(colName).array()
  }

  /*We need to override checks because of the Array not working with contains, so instead we convert
  everything to Strings where we can compare them without issue*/


  override def checkJDriverInsertsCollections() = conn.withSessionDo { session =>
    var resultSet = session.execute(s"SELECT * FROM $typeCollectionTable")
    var rows = resultSet.all().asScala

    rows.size should equal(typeData.length)

    rows.foreach { row =>
      val resSet = row.getSet("set1", classOf[ByteBuffer]).asScala.map(Bytes.toHexString(_))
      val resList = row.getList("list1", classOf[ByteBuffer]).asScala.map(Bytes.toHexString(_))
      val resMap1 = row.getMap("map1", classOf[String], classOf[ByteBuffer]).asScala
        .mapValues(Bytes.toHexString(_))
      val resMap2 = row.getMap("map2", classOf[ByteBuffer], classOf[String]).asScala
        .map{ case (k,v) => (Bytes.toHexString(k), v)}

      resSet should contain theSameElementsAs bufferTypeSet.map(Bytes.toHexString(_))
      resList should contain theSameElementsAs bufferTypeSet.toList.map(Bytes.toHexString(_))
      resMap1 should contain theSameElementsAs bufferTypeMap1.mapValues( Bytes.toHexString(_))
      resMap2 should contain theSameElementsAs bufferTypeMap2.map{ case (k,v) => (Bytes.toHexString(k), v)}
    }
  }

  override def checkNormalRowConsistency(
    expectedKeys: Seq[Array[Byte]],
    results: Seq[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])]) {

    val expectedBytesAsHex = expectedKeys.map(Bytes.toHexString(_))
    val resultBytesAsHex = results.map { case (pkey, ckey1, ckey2, data1) => (
      Bytes.toHexString(pkey),
      Bytes.toHexString(ckey1),
      Bytes.toHexString(ckey2),
      Bytes.toHexString(data1))
    }
    val foundBytesAsHex = resultBytesAsHex.filter( x => expectedBytesAsHex.contains(x._1))
    foundBytesAsHex should contain theSameElementsAs expectedBytesAsHex.map(x => (x, x, x, x))
  }

  override def checkCollectionConsistency(
    expectedKeys: Seq[Array[Byte]],
    expectedCollections: (Set[Array[Byte]], List[Array[Byte]], Map[String, Array[Byte]], Map[Array[Byte], String]),
    results: Seq[(Array[Byte], Set[Array[Byte]], List[Array[Byte]], Map[String, Array[Byte]], Map[Array[Byte], String])]) {

    val expectedBytesAsHex = expectedKeys.map(Bytes.toHexString(_))
    val resultBytesAsString = results.map { case (pkey, set1, list1, map1, map2) => (
      Bytes.toHexString(pkey),
      set1.map(Bytes.toHexString(_)),
      list1.map(Bytes.toHexString(_)),
      map1.mapValues(Bytes.toHexString(_)),
      map2.map{ case (k,v) => (Bytes.toHexString(k), v)})
    }

    val foundBytesAsHex = resultBytesAsString.filter( x => expectedBytesAsHex.contains(x._1))

    foundBytesAsHex should contain theSameElementsAs {
      val expected = expectedBytesAsHex.map(x => (
        x,
        expectedCollections._1.map(Bytes.toHexString(_)),
        expectedCollections._2.map(Bytes.toHexString(_)),
        expectedCollections._3.mapValues(Bytes.toHexString(_)),
        expectedCollections._4.map { case (k, v) => (Bytes.toHexString(k), v) }))
      expected
    }
  }
}

