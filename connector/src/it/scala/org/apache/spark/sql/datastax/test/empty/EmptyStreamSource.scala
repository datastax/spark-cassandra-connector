package org.apache.spark.sql.datastax.test.empty

import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}


/**
  * @author Russell Spitzer
  */
class DefaultSource extends StreamSourceProvider {
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    ("FakeStream", StructType(Seq(StructField("key", IntegerType, nullable = false ))))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String, parameters: Map[String, String]) =
    new Source {

    var offset = new LongOffset(0)
    override def schema: StructType = StructType(Seq(StructField("key", IntegerType, nullable = false )))

    override def getOffset: Option[Offset] = {
      offset = offset + 100L
      Some(offset.copy())
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      Dataset.ofRows(sqlContext.sparkSession, LocalRelation(schema.toAttributes, isStreaming = true))
    }

    override def stop() {}
  }
}

