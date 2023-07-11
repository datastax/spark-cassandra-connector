package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.writer.{TTLOption, TimestampOption, WriteConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{TTLParam, WriteTimeParam}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import scala.util.Try

case class CassandraWriteBuilder(
  session: SparkSession,
  tableDef: TableDef,
  catalogName: String,
  options: CaseInsensitiveStringMap,
  inputSchema: StructType)
  extends WriteBuilder with SupportsTruncate {

  private val consolidatedConf = consolidateConfs(
    session.sparkContext.getConf,
    session.conf.getAll,
    catalogName,
    tableDef.keyspaceName,
    options.asScala.toMap)

  private val initialWriteConf = WriteConf.fromSparkConf(consolidatedConf)

  private val primaryKeyNames = tableDef.primaryKey.map(_.columnName).toSet
  private val inputColumnNames = inputSchema.map(_.name).toSet

  private val missingPrimaryKeyColumns = primaryKeyNames -- inputColumnNames

  if (missingPrimaryKeyColumns.nonEmpty) {
    throw new CassandraCatalogException(
      s"""Attempting to write to C* Table but missing
         |primary key columns: ${missingPrimaryKeyColumns.mkString("[", ",", "]")}""".stripMargin)
  }

  private val ttlWriteOption =
    consolidatedConf.getOption(TTLParam.name)
      .map(value =>
        Try(value.toInt)
          .map(TTLOption.constant)
          .getOrElse(TTLOption.perRow(value)))
      .getOrElse(initialWriteConf.ttl)

  private val timestampWriteOption =
    consolidatedConf.getOption(WriteTimeParam.name)
      .map(value =>
        Try(value.toLong)
          .map(TimestampOption.constant)
          .getOrElse(TimestampOption.perRow(value)))
      .getOrElse(initialWriteConf.timestamp)

  private val connector = CassandraConnector(consolidatedConf)
  private val writeConf = initialWriteConf.copy(ttl = ttlWriteOption, timestamp = timestampWriteOption)

  override def buildForBatch(): BatchWrite = getWrite()

  private def getWrite(): CassandraBulkWrite = {
    CassandraBulkWrite(session, connector, tableDef, writeConf, inputSchema, consolidatedConf)
  }

  override def buildForStreaming(): StreamingWrite = getWrite()

  /**
    * With Cassandra we cannot actually do this before commit since we are writing constantly,
    * our best option is to truncate now. Since we have no notion of rollbacks this is probably
    * the best we can do.
    **/
  override def truncate(): WriteBuilder = {
    if (consolidatedConf.getOption("confirm.truncate").getOrElse("false").toBoolean) {
      connector.withSessionDo(session =>
        session.execute(
          QueryBuilder.truncate(CqlIdentifier.fromInternal(tableDef.keyspaceName), CqlIdentifier.fromInternal(tableDef.tableName)).asCql()))
      this
    } else {
      throw new UnsupportedOperationException(
        """You are attempting to use overwrite mode which will truncate
          |this table prior to inserting data. If you would merely like
          |to change data already in the table use the "Append" mode.
          |To actually truncate please pass in true value to the option
          |"confirm.truncate" or set that value to true in the session.conf
          | when saving. """.stripMargin)
    }
  }
}

case class CassandraBulkWrite(
  session: SparkSession,
  connector: CassandraConnector,
  tableDef: TableDef,
  writeConf: WriteConf,
  inputSchema: StructType,
  consolidatedConf: SparkConf)
  extends BatchWrite
    with StreamingWrite {


  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = getWriterFactory()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {} //No Commit in Cassandra Driver Writes

  override def abort(messages: Array[WriterCommitMessage]): Unit = {} //No Abort possible in Cassandra Driver Writes

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = getWriterFactory()

  private def getWriterFactory(): CassandraDriverDataWriterFactory = {
    CassandraDriverDataWriterFactory(
      connector,
      tableDef,
      inputSchema,
      writeConf
    )
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}
