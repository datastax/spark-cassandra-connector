package com.datastax.spark.connector.doc

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.util.{RefBuilder, ConfigCheck}
import java.io.File
import scala.collection.JavaConversions._

class DocCheck extends SparkCassandraITFlatSpecBase{

  val pwd = System.getenv("PWD")

  val reRunMessage =
    """******* re-run sbt spark-cassandra-connector-unshaded/run to regenerate properties file
      |*******.
    """.stripMargin

  "The Reference Doc File" should "contain all of the current properties" in withClue(reRunMessage){
    val refFile = scala.io.Source.fromFile(new File(s"$pwd/doc/reference.md")).mkString

    val missingProperties =
      for (propertyName <- ConfigCheck.validStaticPropertyNames
        if !refFile.contains(propertyName.stripPrefix("spark.cassandra."))) yield propertyName

    missingProperties should be ('empty)

    info(s"Reference contains ${ConfigCheck.validStaticPropertyNames.size} entries")
  }

  "it" should "match a freshly created reference file" in withClue(reRunMessage){
    val refFile = scala.io.Source.fromFile(new File(s"$pwd/doc/reference.md")).mkString
    RefBuilder.getMarkDown() should be(refFile)

  }

  case class ParameterFound (parameter: String, fileName : String)
  "The Docs" should "only reference current parameters" in {
    val docFiles = new java.io.File(s"$pwd/doc").listFiles()
    val allDocs = docFiles.map( file => (file, scala.io.Source.fromFile(file).mkString))

    val SparkParamRegex = """spark\.cassandra\.\w+""".r.unanchored
    val parameterUsages = for (
      doc <- allDocs;
      m <- SparkParamRegex findAllMatchIn doc._2
    ) yield {
      ParameterFound(m.matched, doc._1.getName)
    }

    val PropertyNames = ConfigCheck.validStaticPropertyNames

    val matchesByFile = parameterUsages.groupBy(x => x.fileName).toList.sortBy( x => x._1)
    for ( fileToMatches <- matchesByFile) {
      info( s"${fileToMatches._1} had ${fileToMatches._2.size} parameter usages")
    }

    info(s"Docs contains ${parameterUsages.size} references to spark.cassandra properties")

    val unknownParameters: Seq[ParameterFound] =
      for (foundParameter <- parameterUsages
        if PropertyNames.contains(foundParameter.parameter)) yield {
      foundParameter
    }
    unknownParameters should be ('empty)
  }

}
