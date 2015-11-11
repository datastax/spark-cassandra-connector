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
  rowReaderNormal: RowReaderFactory[(TestType, TestType, TestType)],
  rowReaderSet: RowReaderFactory[(TestType, Set[TestType])],
  rowReaderMap1: RowReaderFactory[(TestType, Map[String, TestType])],
  rowReaderMap2: RowReaderFactory[(TestType, Map[TestType, String])],
  rowReaderNull: RowReaderFactory[(TestType, TestType, Option[TestType], Set[TestType], Map[TestType, TestType], Seq[TestType])],
  rowWriterNormal: RowWriterFactory[(TestType, TestType, TestType)],
  rowWriterSet: RowWriterFactory[(TestType, Set[TestType])],
  rowWriterMap1: RowWriterFactory[(TestType, Map[String, TestType])],
  rowWriterMap2: RowWriterFactory[(TestType, Map[TestType, String])],
  rowWriterNull: RowWriterFactory[(TestType, TestType, Null, Null, Null, Null)])
  extends SparkCassandraITFlatSpecBase {

  protected val normalTables = Seq("_compound", "_composite", "_compound_cs", "_composite_cs",
    "_cs")
  protected val colTables = Seq("_list", "_set")
  protected val mapTables = Seq("_map_1", "_map_2")

  protected var typeNormalTables: Seq[String] = Nil
  protected var typeColTables: Seq[String] = Nil
  protected var typeMapTables: Seq[String] = Nil

  //Writer Column Names
  private val normCols = SomeColumns("pkey", "ckey1", "data1")
  private val collCols = SomeColumns("pkey", "data1")
  private val nullCols = SomeColumns("pkey", "data1", "nulldata", "nullset", "nulllist", "nullmap")

  //Implement these values for the class extending AbstractTypeTest
  protected val typeName: String
  protected val typeData: Seq[TestType]
  protected val typeSet: Set[TestType]
  protected val typeMap1: Map[String, TestType]
  protected val typeMap2: Map[TestType, String]

  protected val addData: Seq[TestType]
  protected val addSet: Set[TestType]
  protected val addMap1: Map[String, TestType]
  protected val addMap2: Map[TestType, String]

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
    checkSparkReadMaps
  }

  it should "be readable via cassandraTable when nulled " in {
    checkSparkReadNull
  }

  it should "be writable from an RDD via saveToCassandra " in {
    sparkWriteNormal
  }

  it should "be writable from an RDD with collections via saveToCassandra " in {
    sparkWriteCollections
    sparkWriteMaps
  }

  it should "be able to write NULLS to a C* table via saveToCassandra" in {
    sparkWriteNulls
  }

  it should "be readable and writable with DataFrames" in {
    dataframeReadWrite
  }

  override def beforeAll() {
    setKeyspaceTableNames()
    generateKsTable()
    truncateTables()
    insertData()
  }

  def checkJDriverInsertsNormal() = conn.withSessionDo { session =>
    typeNormalTables.foreach { table =>
      val resultSet = session.execute(s"SELECT * FROM $table")
      val rows = resultSet.all().asScala
      rows.size should equal(typeData.length)
      rows.foreach { row =>
        getDriverColumn(row, "pkey") should equal(getDriverColumn(row, "ckey1"))
        getDriverColumn(row, "ckey1") should equal(getDriverColumn(row, "data1"))
      }
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
    var resultSet = session.execute(s"SELECT * FROM ${typeName}_set")
    var rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    val convertedSet = typeSet.map(convertToDriverInsertable(_))
    rows.foreach { row =>
      val resSet = row.getSet("data1", classTag[DriverType].runtimeClass).asScala
      resSet should contain theSameElementsAs convertedSet
    }

    resultSet = session.execute(s"SELECT * FROM ${typeName}_list")
    rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    rows.foreach { row =>
      val resSet = row.getList("data1", classTag[DriverType].runtimeClass).asScala
      resSet should contain theSameElementsAs convertedSet
    }

    resultSet = session.execute(s"SELECT * FROM ${typeName}_map_1")
    rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    val convertedMap1 = typeMap1.mapValues(convertToDriverInsertable(_))
    rows.foreach { row =>
      val resSet = row.getMap("data1", classOf[String], classTag[DriverType].runtimeClass).asScala
      resSet should contain theSameElementsAs convertedMap1
    }

    resultSet = session.execute(s"SELECT * FROM ${typeName}_map_2")
    rows = resultSet.all().asScala
    rows.size should equal(typeData.length)
    val convertedMap2 = typeMap2.map{ case (k,v) => (convertToDriverInsertable(k), v)}
    rows.foreach { row =>
      val resSet = row.getMap("data1", classTag[DriverType].runtimeClass, classOf[String]).asScala
      resSet should contain theSameElementsAs convertedMap2
    }
  }

  def checkSparkReadNormal() {
    typeNormalTables.foreach { table: String =>
      val result = sc.cassandraTable[(TestType, TestType, TestType)](keyspaceName, table).collect
      result.size should equal(typeData.length)
      checkNormalRowConsistency(typeData, result)
    }
  }

  def checkNormalRowConsistency(
    expectedKeys: Seq[TestType],
    foundRows: Seq[(TestType, TestType, TestType)]) {

    val expectedRows = expectedKeys.map(x => (x, x, x)).toSet
    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found: $foundRows Missing:") { missingRows should be ('empty) }
  }

  def checkCollectionConsistency(
    expectedKeys: Seq[TestType],
    expectedSet: Set[TestType],
    foundRows: Seq[(TestType, Set[TestType])]) {

    val expectedRows = expectedKeys.map(x => (x, expectedSet)).toSet
    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found: $foundRows Missing:") { missingRows should be ('empty) }
  }

  def checkMap1Consistenecy(
    expectedKeys: Seq[TestType],
    expectedMap: Map[String, TestType],
    foundRows: Seq[(TestType, Map[String, TestType])]) {

    val expectedRows = expectedKeys.map(x => (x, expectedMap)).toSet
    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found: $foundRows Missing:") { missingRows should be ('empty) }
  }

  def checkMap2Consistenecy(
    expectedKeys: Seq[TestType],
    expectedMap: Map[TestType, String],
    foundRows: Seq[(TestType, Map[TestType, String])]) {

    val expectedRows = expectedKeys.map(x => (x, expectedMap)).toSet
    val missingRows = expectedRows -- foundRows
    withClue(s"We found missing rows in a normal table Found: $foundRows Missing:") { missingRows should be ('empty) }
  }

  def checkSparkReadCollections() {
    val table_collections = Seq(s"${typeName}_set", s"${typeName}_list")
    table_collections.foreach { table =>
      var result = sc.cassandraTable[(TestType, Set[TestType])](keyspaceName, table).collect
      result.size should equal(typeData.length)
      checkCollectionConsistency(typeData, typeSet, result)
    }
  }

  def checkSparkReadNull() {
    var table = s"${typeName}_null"
    var result = sc
      .cassandraTable[(TestType, TestType, Option[TestType], Set[TestType], Map[TestType, TestType], Seq[TestType])](keyspaceName, table)
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

  def checkSparkReadMaps() {
    var table = s"${typeName}_map_1"
    val resultMap1 = sc.cassandraTable[(TestType, Map[String,TestType])](keyspaceName, table)
      .collect
    resultMap1.size should equal(typeData.length)
    checkMap1Consistenecy(typeData, typeMap1, resultMap1)

    table = s"${typeName}_map_2"
    val resultMap2 = sc.cassandraTable[(TestType, Map[TestType, String])](keyspaceName, table)
      .collect
    resultMap2.size should equal(typeData.length)
    checkMap2Consistenecy(typeData, typeMap2, resultMap2)
  }

  def sparkWriteNormal() {
    typeNormalTables.foreach { table =>
      val writeArray = addData.map(value => (value, value, value))
      val writeRdd = sc.parallelize(writeArray)
      writeRdd.saveToCassandra(keyspaceName, table, normCols)
      val rdd = sc.cassandraTable[(TestType,TestType,TestType)](keyspaceName, table).collect
      rdd.size should equal(addData.length + typeData.length)
      checkNormalRowConsistency(addData, rdd)
    }
  }

  def sparkWriteCollections() {
    typeColTables.foreach { table =>
      val writeArray = addData.map(value => (value, addSet))
      val writeRdd = sc.parallelize(writeArray)
      writeRdd.saveToCassandra(keyspaceName, table, collCols)
      val rdd = sc.cassandraTable[(TestType, Set[TestType])](keyspaceName, table).collect
      rdd.size should equal(addData.length + typeData.length)
      checkCollectionConsistency(addData, addSet, rdd)
    }
  }

  def sparkWriteMaps() {
    val writeRdd1 = sc.parallelize(addData.map(value => (value, addMap1)))
    val writeRdd2 = sc.parallelize(addData.map(value => (value, addMap2)))
    writeRdd1.saveToCassandra(keyspaceName, s"${typeName}_map_1", collCols)
    writeRdd2.saveToCassandra(keyspaceName, s"${typeName}_map_2", collCols)

    val resultMap1 = sc.cassandraTable[(TestType, Map[String, TestType])](keyspaceName,
    s"${typeName}_map_1").collect
    resultMap1.size should equal(addData.length + typeData.length)
    checkMap1Consistenecy(addData, addMap1, resultMap1)

    val resultMap2 = sc.cassandraTable[(TestType, Map[TestType, String])](keyspaceName,
    s"${typeName}_map_2").collect
    resultMap2.size should equal(addData.length + typeData.length)
    checkMap2Consistenecy(addData, addMap2, resultMap2)
  }

  def sparkWriteNulls() {
    val table = s"${typeName}_null"
    val writeArray = addData.map(value => (value, value, null, null, null, null))
    val writeRdd = sc.parallelize(writeArray)
    writeRdd.saveToCassandra(keyspaceName, table, nullCols)
    val result = sc.cassandraTable[(TestType, TestType, Option[TestType], Set[TestType],Map[TestType, TestType], Seq[TestType])](keyspaceName, table).collect
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
      "table" -> s"${typeName}_compound"
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

    val dataCopied = sc.cassandraTable[(TestType, TestType, TestType)](keyspaceName,s"${typeName}_dataframe").collect
    checkNormalRowConsistency(typeData, dataCopied)
  }

  /**
   * A method to insert expectedKeys which will be used during the typeTests test using the java
   * driver
   */
  def insertData() = conn.withSessionDo { session =>

    typeNormalTables.foreach { table =>
      val statement = session.prepare(
        s"""INSERT INTO $keyspaceName.$table
           |(pkey, ckey1, data1)
           |VALUES (?,?,?)""".stripMargin)
      typeData.foreach { value: TestType =>
        val driverValue = convertToDriverInsertable(value)
        val bs = statement.bind(driverValue, driverValue, driverValue)
        session.execute(bs)
      }
    }

    val setTable = s"${typeName}_set"
    val setStatement = session.prepare(
      s"""INSERT INTO $keyspaceName.$setTable
         |(pkey, data1)
         |VALUES (?,?)""".stripMargin)
    val javaSet: java.util.Set[DriverType] = typeSet.map(convertToDriverInsertable(_)).asJava
    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val bs = setStatement.bind(driverValue, javaSet)
      session.execute(bs)
    }

    var listTable = s"${typeName}_list"
    val listStatement = session.prepare(
      s"""INSERT INTO $keyspaceName.$listTable
         |(pkey, data1)
         |VALUES (?,?)""".stripMargin)
    val javaList: java.util.List[DriverType] = typeSet.toSeq.map(convertToDriverInsertable(_))
      .asJava
    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val bs = listStatement.bind(driverValue, javaList)
      session.execute(bs)
    }

    val map1stat = session.prepare(
      s"""INSERT INTO $keyspaceName.${typeName}_map_1
         |(pkey, data1)
         |VALUES (?,?)""".stripMargin)
    val map2stat = session.prepare(
      s"""INSERT INTO $keyspaceName.${typeName}_map_2
         |(pkey, data1)
         |VALUES (?,?)""".stripMargin)
    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val statement = map1stat.bind(
        driverValue, typeMap1.mapValues(value => convertToDriverInsertable(value)).asJava)
      session.execute(statement)
    }
    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val statement = map2stat.bind(
        driverValue, typeMap2.map(kv => (convertToDriverInsertable(kv._1), kv._2)).asJava)
      session.execute(statement)
    }

    val statement = session.prepare(
      s"""INSERT INTO $keyspaceName.${typeName}_null
         |(pkey, data1, nulldata, nullset, nulllist, nullmap)
         |VALUES (?,?,?,?,?,?)""".stripMargin)
    typeData.foreach { value =>
      val driverValue = convertToDriverInsertable(value)
      val bs = statement.bind(driverValue, driverValue, null, null, null, null)
      session.execute(bs)
    }
  }

  def setKeyspaceTableNames() {
    typeNormalTables = normalTables.map(typeName + _)
    typeColTables = colTables.map(typeName + _)
    typeMapTables = mapTables.map(typeName + _)
  }

  def generateKsTable() = conn.withSessionDo { session =>
    session.execute(s"""DROP KEYSPACE IF EXISTS $keyspaceName""")
    session.execute(
      s"""CREATE KEYSPACE IF NOT EXISTS $keyspaceName
         |with replication = {'class':'SimpleStrategy','replication_factor':'1'}""".stripMargin)
    session.execute(s"""USE $keyspaceName""")

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_compound
         |(pkey $typeName, ckey1 $typeName, data1 $typeName , PRIMARY KEY (pkey,ckey1))"""
        .stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_dataframe
         |(pkey $typeName, ckey1 $typeName, data1 $typeName , PRIMARY KEY (pkey,ckey1))"""
        .stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_composite
         |(pkey $typeName, ckey1 $typeName, data1 $typeName , PRIMARY KEY ((pkey,ckey1)))"""
        .stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_compound_cs
         |(pkey $typeName, ckey1 $typeName, data1 $typeName , PRIMARY KEY (pkey,ckey1))
         |WITH COMPACT STORAGE""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_composite_cs
         |(pkey $typeName, ckey1 $typeName, data1 $typeName , PRIMARY KEY ((pkey,ckey1)))
         |WITH COMPACT STORAGE""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_cs
         |(pkey $typeName, ckey1 $typeName, data1 $typeName, PRIMARY KEY (pkey))
         |WITH COMPACT STORAGE""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_list
         |(pkey $typeName, data1 list<$typeName>, PRIMARY KEY (pkey) )""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_set
         |(pkey $typeName, data1 set<$typeName>, PRIMARY KEY (pkey) )""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_map_1
         |(pkey $typeName, data1 map<ascii,$typeName>, PRIMARY KEY (pkey))""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_map_2
         |(pkey $typeName, data1 map<$typeName,ascii>, PRIMARY KEY (pkey))""".stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${typeName}_null
         |(pkey $typeName,
         |data1 $typeName,
         |nulldata $typeName,
         |nullset set<$typeName>,
         |nulllist list<$typeName>,
         |nullmap map<$typeName,$typeName>,
         |PRIMARY KEY (pkey))""".stripMargin)
  }

  def truncateTables() = conn.withSessionDo { session =>
    typeNormalTables.foreach(table =>
      session.execute(s"TRUNCATE $table")
    )
    typeColTables.foreach(table =>
      session.execute(s"TRUNCATE $table")
    )
    typeMapTables.foreach(
      table =>
        session.execute(s"TRUNCATE $table")
    )
    session.execute(s"TRUNCATE ${typeName}_null")
  }

}

