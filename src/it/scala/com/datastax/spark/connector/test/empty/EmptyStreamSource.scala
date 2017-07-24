package com.datastax.spark.connector.test.empty

import com.datastax.spark.connector.test.monotonic.FakeStreamSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


/**
  * @author Russell Spitzer
  */
class DefaultSource extends StreamSourceProvider {
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    ("FakeStream", StructType(Seq(StructField("key", IntegerType, false ))))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String, parameters: Map[String, String]) =
    new Source {

    var offset = new LongOffset(0)
    override def schema: StructType = StructType(Seq(StructField("key", IntegerType, false )))

    override def getOffset: Option[Offset] = {
      offset = offset + 100L
      Some(offset.copy())
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      sqlContext.sparkSession.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], schema)
    }

    override def stop() {}
  }
}

