package com.datastax.spark.connector.test.monotonic

import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Try


class DefaultSource extends StreamSourceProvider {

  override def createSource(
                             spark: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    new Source {
      var offset = new LongOffset(0)
      override def schema: StructType = StructType(Seq(StructField("key", IntegerType, false )))

      override def getOffset: Option[Offset] = {
        offset = offset + 100L
        Some(offset.copy())
      }

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        import spark.implicits._

        val startValue = start match {
          case Some(ser: SerializedOffset) => FakeStreamSource.parseOffset(ser.json)
          case Some(LongOffset(x)) => x
          case None => 0
          case _ => -1
        }

        val endValue = end match {
          case ser: SerializedOffset => FakeStreamSource.parseOffset(ser.json)
          case LongOffset(x) => x
        }

        (startValue.toInt to endValue.toInt).toDS().toDF()
      }

      override def stop() {}
    }
  }

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    ("FakeStream", StructType(Seq(StructField("key", IntegerType, false ))))
  }
}

object FakeStreamSource {
  def parseOffset(str: String): Long = {
    Try (str.stripPrefix("[").stripSuffix("]").toLong) getOrElse (0)
  }
}
