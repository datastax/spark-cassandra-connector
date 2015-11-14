package com.datastax.spark.connector.rdd.typeTests

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.SparkTemplate._
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

import com.datastax.spark.connector.types.TypeConverter
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._
import scala.reflect._

/**
 * A template class for testing that various CQL Types work with the spark Cassandra Connector
 * When creating an iimplementationof this test you must provide two types,
 * The TestType is used to extract values from Cassandra ie: sc.cassandraTable[TestType]
 * and saving values in cassandra via sc.parallelize(x:TesType).saveToCassandra()
 * The DriverType is used for inserting values into C* via the javaDriver
 *
 * The convertToDriverInsertable Function should be overridden if there isn't an implicit
 * transformation between TestType and DriverType (See BlobTypeTest for an example)
 */
abstract class AbstractTypeTest[TestType: ClassTag, DriverType <: AnyRef : ClassTag](
  implicit
  tConverter: TypeConverter[TestType],
  rowReaderNormal: RowReaderFactory[(TestType, TestType, TestType, TestType)],
  rowReaderCollection: RowReaderFactory[(TestType, Set[TestType], List[TestType], Map[String, TestType], Map[TestType, String])],
  rowReaderNull: RowReaderFactory[(TestType, TestType, Option[TestType], Set[TestType], Map[TestType, TestType], Seq[TestType])],
  rowWriterNormal: RowWriterFactory[(TestType, TestType, TestType, TestType)],
  rowWriterCollection: RowWriterFactory[(TestType, Set[TestType], List[TestType], Map[String, TestType], Map[TestType, String])],
  rowWriterNull: RowWriterFactory[(TestType, TestType, Null, Null, Null, Null)])
  extends SparkCassandraITFlatSpecBase {

  protected lazy val typeNormalTable: String = s"${typeName}_normal"
  protected lazy val typeCollectionTable: String = s"${typeName}_collection"
  protected lazy val typeNullTable: String = s"${typeName}_null"

  //Writer Column Names
  private val normCols = SomeColumns("pkey", "ckey1", "ckey2", "data1")
  private val collCols = SomeColumns("pkey", "set1", "list1", "map1", "map2")
  private val nullCols = SomeColumns("pkey", "data1", "nulldata", "nullset", "nulllist", "nullmap")

  //Implement these values for the class extending AbstractTypeTest
  protected val typeName: String
  protected val typeData: Seq[TestType]
  protected def typeSet: Set[TestType] = typeData.toSet
  protected def typeMap: Map[String, TestType] = typeData
    .zipWithIndex.map( pair => (s"Element ${pair._2}", pair._1)).toMap

  protected def typeReverseMap: Map[TestType, String] = typeMap.map(_.swap)

  protected val addData: Seq[TestType]
  protected def addSet: Set[TestType] = addData.toSet
  protected def addMap: Map[String, TestType] = addData
    .zipWithIndex.map( pair => (s"Element ${pair._2}", pair._1)).toMap

  protected def addReverseMap: Map[TestType, String]= addMap.map(_.swap)

  protected val keyspaceName: String = s"typetest_ks"

  /**
   * Override this function if TestType is different than DriverType
   */
  def convertToDriverInsertable(testValue: TestType): DriverType = testValue match {
    case javaValue: DriverType => javaValue
    case _ => throw new UnsupportedOperationException(
      """Type Test Must Implement a Way of Turning the Representative Type to a Java Class
        |the Driver Can Insert""".stripMargin)
  }

  /**
   * Java doesn't have the fun typeTests magic that Scala does so we'll need to use the driver
   * specific function for each expectedKeys typeTests
   */
  def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): TestType

  val sparkConf = defaultSparkConf.clone()

  useCassandraConfig(Seq("cassandra-default.yaml.template"))
  useSparkConf(sparkConf)

  lazy val sqlContext = new SQLContext(sc)

  val conn = CassandraConnector(defaultConf)

  s"This type" should "be insertable via the CassandraConnector" in {
    checkJDriverInsertsNormal
    checkJDriverInsertsCollections
    checkJDriverInsertsNull
  }

  it should "be readable via cassandraTable" in {
    checkSparkReadNormal
  }

  it should "be readable via cassandraTable in Collections" in {
    checkSparkReadCollections
  }

  it should "be readable via cassandraTable when nulled " in {
    checkSparkReadNull
  }

  it should "be writable from an RDD via saveToCassandra " in {
    sparkWriteNormal
  }

  it should "be writable from an RDD with collections via saveToCassandra " in {
    sparkWriteCollections
  }

  it should "be able to write NULLS to a C* table via saveToCassandra" in {
    sparkWriteNulls
  }

  it should "be readable and writable with DataFrames" in {
    dataframeReadWrite
  }

  override def beforeAll() {
    generateKsTable()
    truncateTables()
    insertData()
  }

  def checkJDriverInsertsNormal() = conn.withSessionDo { session =>
      val resultSet = session.execute(s"SELECT * FROM $typeNormalTable")
      val rows = resultSet.all().asScala
      rows.size should equal(typeData.length)
      rows.foreach { row =>
        getDriverColumn(row, "pkey") should equal(getDriverColumn(row, "ckey1"))
        getDriverColumn(row, "ckey2") should equal(getDriverColumn(row, "ckey1"))
        getDriverColumn(row, "ckey1") should equal(getDriverColumn(row, "data1"))
      }
  }

  def checkJDriverInsertsNull() = conn.withSessionDo { session =>
    val table = s"${typeName}_null"
    val resultSet = session.execute(s"SELECT * FROM $table")
    val rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    rows.foreach { row =>
      row.isNull("nulldata") should be(true)
      row.isNull("nullset") should be(true)
      row.isNull("nulllist") should be(true)
      row.isNull("nullmap") should be(true)
    }
  }

  def checkJDriverInsertsCollections() = conn.withSessionDo { session =>
    var resultSet = session.execute(s"SELECT * FROM $typeCollectionTable")
    var rows = resultSet.all().asScala

    rows.size should equal(typeData.length)

    val convertedSet = typeSet.map(convertToDriverInsertable(_))
    val convertedMap1 = typeMap.mapValues(convertToDriverInsertable(_))
    val convertedMap2 = convertedMap1.map(_.swap)

    rows.foreach { row =>
      val resSet = row.getSet("set1", classTag[DriverType].runtimeClass).asScala
      val resList = row.getList("list1", classTag[DriverType].runtimeClass).asScala
      val resSetMap1 = row.getMap("map1", classOf[String], classTag[DriverType].runtimeClass).asScala
      val resSetMap2 = row.getMap("map2", classTag[DriverType].runtimeClass, classOf[String]).asScala

      resSetMap1 should contain theSameElementsAs convertedMap1
      resSetMap2 should contain theSameElementsAs convertedMap2
      resSet should contain theSameElementsAs convertedSet
      resList should contain theSameElementsAs convertedSet
    }
  }

  def checkSparkReadNormal() {
    val result = sc.cassandraTable[(TestType, TestType, TestType, TestType)](keyspaceName, typeNormalTable).collect
    result.size should equal(typeData.length)
    checkNormalRowConsistency(typeData, result)
  }

  def checkNormalRowConsistency(
    expectedKeys: Seq[TestType],
    foundRows: Seq[(TestType, TestType, TestType, TestType)]) {

    val expectedRows = expectedKeys.map(x => (x, x, x, x)).toSet
    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found: $foundRows Missing:") { missingRows should be ('empty) }
  }

  def checkCollectionConsistency(
    expectedKeys: Seq[TestType],
    expectedCollections: (Set[TestType], List[TestType], Map[String, TestType], Map[TestType, String]),
    foundRows: Seq[(TestType, Set[TestType], List[TestType], Map[String, TestType], Map[TestType, String])]) {

    val expectedRows = expectedKeys.map(x => (
      x,
      expectedCollections._1,
      expectedCollections._2,
      expectedCollections._3,
      expectedCollections._4)).toSet

    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found:\n ${foundRows.mkString("\n")}\n Expected:\n ${expectedRows.mkString("\n")}\n") { missingRows should be ('empty) }
  }

  def checkMapConsistenecy(
    expectedKeys: Seq[TestType],
    expectedMap: Map[String, TestType],
    foundRows: Seq[(TestType, Map[String, TestType], Map[TestType, String])]) {

    val expectedRows = expectedKeys.map(x => (x, expectedMap, expectedMap.map(_.swap))).toSet
    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found: ${foundRows.mkString("\n")}\n Missing:") { missingRows should be ('empty) }
  }

  def checkSparkReadCollections() {
    var result = sc.cassandraTable[(
      TestType,
      Set[TestType],
      List[TestType],
      Map[String, TestType],
      Map[TestType, String])](keyspaceName, typeCollectionTable).select(collCols.columns: _*).collect

    result.size should equal(typeData.length)
    checkCollectionConsistency(typeData, (typeSet, typeSet.toList, typeMap, typeReverseMap), result)
  }

  def checkSparkReadNull() {
    var result = sc
      .cassandraTable[(TestType, TestType, Option[TestType], Set[TestType], Map[TestType, TestType], Seq[TestType])](keyspaceName, typeNullTable)
      .collect

    result.size should equal(typeData.length)
    var keys = result.map {
      case (
        pkey,
        data,
        nulldata,
        nullset,
        nullmap,
        nulllist) => {

        nulldata should be(None)
        nullset should be(Set.empty)
        nullmap should be(Map.empty)
        nulllist should be(List.empty)
      }
    }
  }

  def sparkWriteNormal() {
    val writeArray = addData.map(value => (value, value, value, value))
    val writeRdd = sc.parallelize(writeArray)
    writeRdd.saveToCassandra(keyspaceName, typeNormalTable, normCols)
    val rdd = sc.cassandraTable[(TestType, TestType, TestType, TestType)](keyspaceName, typeNormalTable).collect
    rdd.size should equal(addData.length + typeData.length)
    checkNormalRowConsistency(addData, rdd)
  }

  def sparkWriteCollections() {
    val writeArray = addData.map(value => (value, addSet, addSet.toList, addMap, addReverseMap))
    val writeRdd = sc.parallelize(writeArray)
    writeRdd.saveToCassandra(keyspaceName, typeCollectionTable, collCols)
    val rdd = sc.cassandraTable[(
      TestType,
      Set[TestType],
      List[TestType],
      Map[String, TestType],
      Map[TestType, String])](keyspaceName, typeCollectionTable).select(collCols.columns: _*).collect
    rdd.size should equal(addData.length + typeData.length)
    checkCollectionConsistency(addData, (addSet, addSet.toList, addMap, addReverseMap), rdd)
  }

  def sparkWriteNulls() {
    val writeArray = addData.map(value => (value, value, null, null, null, null))
    val writeRdd = sc.parallelize(writeArray)
    writeRdd.saveToCassandra(keyspaceName, typeNullTable, nullCols)
    val result = sc.cassandraTable[(
      TestType,
      TestType,
      Option[TestType],
      Set[TestType],
      Map[TestType,
      TestType],
      Seq[TestType])](keyspaceName, typeNullTable).collect
    result.size should equal(addData.length + typeData.length)

    var keys = result.map {
      case (
        pkey,
        data,
        nulldata,
        nullset,
        nullmap,
        nulllist) => {

        nulldata should be(None)
        nullset should be(Set.empty)
        nullmap should be(Map.empty)
        nulllist should be(List.empty)
      }
    }
  }

  def dataframeReadWrite(): Unit = {
    val cassandraFormat = "org.apache.spark.sql.cassandra"

    val readTableOptions = Map(
      "keyspace" -> keyspaceName,
      "table" -> typeNormalTable
    )

    val writeTableOptions = Map(
      "keyspace" -> keyspaceName,
      "table" -> s"${typeName}_dataframe"
    )

    val readDF = sqlContext
      .read
      .format(cassandraFormat)
      .options(readTableOptions)
      .load()

    readDF
      .write
      .format(cassandraFormat)
      .options(writeTableOptions)
      .save()

    val dataCopied = sc.cassandraTable[(TestType, TestType, TestType, TestType)](keyspaceName,s"${typeName}_dataframe").collect
    checkNormalRowConsistency(typeData, dataCopied)
  }

  /**
   * A method to insert expectedKeys which will be used during the typeTests test using the java
   * driver
   */
  def insertData() = conn.withSessionDo { session =>

    val normalStatement = session.prepare(
      s"""INSERT INTO $keyspaceName.$typeNormalTable
         |(pkey, ckey1, ckey2, data1)
         |VALUES (?,?,?,?)""".stripMargin)
    typeData.foreach { value: TestType =>
      val driverValue = convertToDriverInsertable(value)
      val bs = normalStatement.bind(driverValue, driverValue, driverValue, driverValue)
      session.execute(bs)
    }

    val collectionStatement = session.prepare(
      s"""INSERT INTO $keyspaceName.$typeCollectionTable
         |(pkey, set1, list1, map1, map2)
         |VALUES (?,?,?,?,?)""".stripMargin)
    val scalaSet = typeSet.map(convertToDriverInsertable(_))
    val javaSet: java.util.Set[DriverType] = scalaSet.asJava
    val javaList: java.util.List[DriverType] = scalaSet.toList.asJava

    val javaMap1: java.util.Map[String, DriverType] = typeMap.mapValues(value =>
      convertToDriverInsertable(value)).asJava
    val javaMap2: java.util.Map[DriverType, String] = typeReverseMap.map { case (key, value) =>
      (convertToDriverInsertable(key), value)
    }.asJava

    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val bs = collectionStatement.bind(driverValue, javaSet, javaList, javaMap1, javaMap2)
      session.execute(bs)
    }

    val nullStatement = session.prepare(
      s"""INSERT INTO $keyspaceName.${typeName}_null
         |(pkey, data1, nulldata, nullset, nulllist, nullmap)
         |VALUES (?,?,?,?,?,?)""".stripMargin)
    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val bs = nullStatement.bind(driverValue, driverValue, null, null, null, null)
      session.execute(bs)
    }
  }

  def generateKsTable() = conn.withSessionDo { session =>
    val start = System.currentTimeMillis()
    session.execute(s"""DROP KEYSPACE IF EXISTS $keyspaceName""")

    session.execute(
      s"""CREATE KEYSPACE IF NOT EXISTS $keyspaceName
         |with replication = {'class':'SimpleStrategy','replication_factor':'1'}""".stripMargin)
    session.execute(s"""USE $keyspaceName""")

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_dataframe
         |(pkey $typeName, ckey1 $typeName, ckey2 $typeName, data1 $typeName , PRIMARY KEY ((pkey,ckey1), ckey2))"""
        .stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_normal
         |(pkey $typeName, ckey1 $typeName, ckey2 $typeName, data1 $typeName , PRIMARY KEY ((pkey,ckey1), ckey2))"""
        .stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_collection
         |(pkey $typeName,
         |set1 set<$typeName>,
         |list1 list<${typeName}>,
         |map1 map<ascii,$typeName>,
         |map2 map<$typeName, ascii>,
         |PRIMARY KEY (pkey))""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_null
         |(pkey $typeName,
         |data1 $typeName,
         |nulldata $typeName,
         |nullset set<$typeName>,
         |nulllist list<$typeName>,
         |nullmap map<$typeName,$typeName>,
         |PRIMARY KEY (pkey))""".stripMargin)
    val end = System.currentTimeMillis()
    println(s"Took ${end-start} ms to setup tables")
  }

  def truncateTables() = conn.withSessionDo { session =>
    session.execute(s"TRUNCATE $typeNormalTable")
    session.execute(s"TRUNCATE $typeCollectionTable")
    session.execute(s"TRUNCATE $typeNullTable")
  }

}

