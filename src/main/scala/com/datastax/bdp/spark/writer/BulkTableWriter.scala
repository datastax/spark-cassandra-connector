/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.spark.writer

import java.io.{File, IOException}
import java.nio.file.Path
import java.util.{ArrayList, UUID}

import scala.collection.immutable.IndexedSeq
import scala.collection.{Iterator, Seq}
import scala.util.{Failure, Try}

import com.typesafe.scalalogging.StrictLogging
import org.apache.cassandra.schema.ColumnMetadata.ClusteringOrder
import org.apache.cassandra.schema.{CQLTypeParser, ColumnMetadata, SchemaConstants, SchemaKeyspace, TableId, TableMetadata, TableMetadataRef, Types}
import org.apache.cassandra.cql3.ColumnIdentifier
import org.apache.cassandra.db.marshal.ReversedType
import org.apache.cassandra.dht.{IPartitioner, Range}
import org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter.NullOutputHandler
import org.apache.cassandra.io.sstable.{CQLSSTableWriter, SSTableLoader}
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.streaming.StreamState
import org.apache.cassandra.utils.FBUtilities
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import com.datastax.driver.core.Row
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types.{ColumnType, TypeConverter}
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.writer.{RowWriter, RowWriterFactory}

/** Configuration options for `bulkSaveToCassandra` */
case class BulkWriteConf(outputDirectory: Option[Path] = None,
  deleteSource: Boolean = true,
  bufferSizeInMB: Int = 64)

/** BulkTableWriter writes data directly to sstables in a local directory and
  * then streams sstables to the Cassandra cluster.
  *
  * @param connector used to fetch cluster table, as well as
  *                  to connect to the endpoints to stream data to
  * @param tableDef schema of the table to write to, the table must exist in Cassandra
  * @param rowWriter extracts individual column values from each RDD item
  * @param outputDirectory temporary, writeable  directory to dump SSTables into
  * @param deleteSource if true, the sstables will be deleted after successful streaming
  * @param bufferSizeInMB the larger the buffer, the more memory it takes on the client,
  *                       but produces bigger sstables, so less compaction on the C* side is needed */
class BulkTableWriter[T](connector: CassandraConnector,
  tableDef: TableDef,
  rowWriter: RowWriter[T],
  outputDirectory: File,
  deleteSource: Boolean = true,
  bufferSizeInMB: Int = 64) extends Serializable with Logging {

  private val keyspaceName = tableDef.keyspaceName
  private val tableName = tableDef.tableName
  private val columnNames = rowWriter.columnNames
  private val columns = columnNames.map(tableDef.columnByName)

  private def quote(name: String): String = "\"" + name + "\""
  private val insertStatement: String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(_ => "?").mkString(", ")
    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec)".trim
  }

  /** Creates the output directory where the sstables for the given Spark partition will be saved.
    * The task output directory is relative to the output directory of the `BulkTableWriter`:
    * outputDirectory / keyspaceName / tableName-partitionId  */
  private def createTaskOutputDirectory(taskContext: TaskContext): File = {
    val taskOutputDirectory =
      new File(new File(outputDirectory, keyspaceName), tableName + "-" + taskContext.partitionId().toString)
    if (!taskOutputDirectory.exists() && !taskOutputDirectory.mkdirs())
      throw new IOException(s"Failed to create directory or one of its parents: $taskOutputDirectory")
    taskOutputDirectory
  }

  /** Creates an SSTableWriter which will dump rows into a local SSTable */
  private def createTableWriter(taskOutputDir: File): CQLSSTableWriter = {

    DatabaseDescriptor.toolInitialization(false)

    val partitioner =
      connector.withClusterDo { cluster =>
        FBUtilities.newPartitioner(cluster.getMetadata.getPartitioner)
      }

    CQLSSTableWriter.builder()
      .forTable(tableDef.cql)
      .using(insertStatement)
      .withPartitioner(partitioner)
      .inDirectory(taskOutputDir)
      .withBufferSizeInMB(bufferSizeInMB)
      .build()
  }

  /** Returns `TypeConverter` objects for each column that will be saved.
    * Column values must be converted to appropriate Java object types expected by `CqlSSTableWriter`.
    * E.g. Scala collections must be converted to Java collections or `None` converted to null.
    * Entries of the resulting sequence are in the same order as the `columns` sequence, so the first
    * type converter corresponds to the first column, and so on. */
  private def getColumnConverters: Seq[TypeConverter[_ <: AnyRef]] = {
    connector.withClusterDo { cluster =>
      val tableMetaData = cluster.getMetadata.getKeyspace(quote(keyspaceName)).getTable(quote(tableName))
      for (c <- columns) yield {
        val columnType = tableMetaData.getColumn(c.columnName).getType
        ColumnType.converterToCassandra(columnType)
      }
    }
  }

  /** `ExternalClient` does two things:
    * 1. it gives `SSTableLoader` the node to token range mapping,
    * so that `SSTableLoader` knows where to stream data to;
    * 2. it gives `SSTableLoader` table table,
    * so that Cassandra knows which table the streamed data should be merged into. */
  private class ExternalClient extends SSTableLoader.Client {

    private var metadata: TableMetadata = null
    private var keyspace: String = null
    private var partitioner: IPartitioner  = null

    def addTokenRanges(): Unit = {
      import scala.collection.JavaConversions._
      connector.withClusterDo { cluster =>
        val metadata = cluster.getMetadata
        partitioner = FBUtilities.newPartitioner(metadata.getPartitioner())
        val tokenRanges = metadata.getTokenRanges
        val tokenFactory = partitioner.getTokenFactory
        for (tokenRange <- tokenRanges) {
          val endpoints = metadata.getReplicas(quote(keyspace), tokenRange)
          val range = new Range(
              tokenFactory.fromString(tokenRange.getStart.getValue.toString),
              tokenFactory.fromString(tokenRange.getEnd.getValue.toString))
          for (endpoint <- endpoints) {
            addRangeForEndpoint(range, endpoint.getAddress)
          }
        }
      }
    }

    def fetchCfMetaData(): TableMetadata = {
      val tableQuery =
          s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.TABLES} " +
            s"WHERE keyspace_name = ? AND table_name = ? "
      val typeQuery = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.TYPES} WHERE keyspace_name = ?"
      val columnsQuery = s"SELECT * FROM ${SchemaConstants.SCHEMA_KEYSPACE_NAME}.${SchemaKeyspace.COLUMNS} " +
            s"WHERE keyspace_name = ? AND table_name = ?"

      connector.withSessionDo { session =>
        val row = session.execute(tableQuery, keyspaceName, tableName).one
        val id = row.getUUID("id")

        import scala.collection.JavaConversions._
        val rawTypes = Types.rawBuilder(keyspaceName);
        for (row <- session.execute(typeQuery, keyspaceName).all) {
            val name = row.getString("type_name")
            val fieldNames = row.getList("field_names", classOf[String])
            val fieldTypes = row.getList("field_types", classOf[String])
            rawTypes.add(name, fieldNames, fieldTypes)
        }
        val types = rawTypes.build()

        val defs = new ArrayList[ColumnMetadata]()
        for (colRow <- session.execute(columnsQuery, keyspaceName, tableName).all)
            defs.add(createDefinitionFromRow(colRow, keyspace, tableName, types))

        TableMetadata.builder(keyspace, tableName, TableId.fromUUID(id))
            .flags(TableMetadata.Flag.fromStringSet(row.getSet("flags", classOf[String])))
            .addColumns(defs)
            .partitioner(partitioner).build();
      }
    }

    private def createDefinitionFromRow(row: Row, keyspace: String, table: String, types: Types): ColumnMetadata = {
      val order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase())
      var columnType = CQLTypeParser.parse(keyspace, row.getString("type"), types)
      if (order == ClusteringOrder.DESC) columnType = ReversedType.getInstance(columnType)
      val name = ColumnIdentifier.getInterned(columnType, row.getBytes("column_name_bytes"), row.getString("column_name"))

      val position = row.getInt("position")
      val kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase())
      new ColumnMetadata(keyspace, table, name, columnType, position, kind)
    }

    override def init(keyspaceName: String) = {
      if (keyspaceName != BulkTableWriter.this.keyspaceName)
        throw new NoSuchElementException(s"Unknown keyspace: $keyspaceName")
      keyspace = keyspaceName
      addTokenRanges()
      metadata = fetchCfMetaData()
    }

    override def getTableMetadata(table: String): TableMetadataRef = {
      if (table != BulkTableWriter.this.tableName)
        throw new NoSuchElementException(s"Unknown table: $table")
      TableMetadataRef.forOfflineTools(metadata)
    }

  }

  /** Creates the `SSTableLoader` which is responsible for
    * streaming the SSTable file to the Cassandra nodes. */
  private def createTableLoader(taskOutputDir: File): SSTableLoader = {
    new SSTableLoader(taskOutputDir, new ExternalClient, new NullOutputHandler) {
      override def onSuccess(finalState: StreamState) {
        if (deleteSource)
          FileUtils.deleteRecursive(taskOutputDir)
      }
    }
  }

  private def convertColumnValues(converters: IndexedSeq[TypeConverter[_ <: AnyRef]],
    src: Array[Any], dest: Array[AnyRef]): Unit = {
    for (i <- columns.indices)
      dest(i) = converters(i).convert(src(i))
  }

  private def writeSSTables(data: scala.Iterator[T], taskOutputDir: File): Unit = {

    val columnConverters = getColumnConverters.toIndexedSeq
    val sstableWriter = createTableWriter(taskOutputDir)
    val rowBuffer = Array.ofDim[Any](columns.size)
    val convertedRowBuffer = Array.ofDim[AnyRef](columns.size)
    try {
      for (row <- data) {
        rowWriter.readColumnValues(row, rowBuffer)
        convertColumnValues(columnConverters, rowBuffer, convertedRowBuffer)
        sstableWriter.addRow(convertedRowBuffer: _*)
      }
    }
    finally {
      sstableWriter.close()
    }
  }

  private def streamSSTables(taskOutputDir: File): Unit = {
    val sstableLoader = createTableLoader(taskOutputDir)
    sstableLoader.stream().get()
    if (sstableLoader.getFailedHosts.size() > 0)
      throw new IOException(s"Streaming of SSTables to some Cassandra hosts has failed: ${sstableLoader.getFailedHosts}")
  }


  def write(taskContext: TaskContext, data: Iterator[T]) {
    val taskOutputDir = createTaskOutputDirectory(taskContext)
    logInfo(s"Writing sstables for partition ${taskContext.partitionId()} to $taskOutputDir...")
    writeSSTables(data, taskOutputDir)
    logInfo(s"Streaming sstables for partition ${taskContext.partitionId()} to Cassandra nodes...")
    streamSSTables(taskOutputDir)
    logInfo(s"Streaming sstables for partition ${taskContext.partitionId()} finished.")
  }

}

object BulkTableWriter extends StrictLogging {
  // We want to give the user a informative message why BulkTableWriter may not work
  val cl = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
  Try(cl.loadClass("org.apache.cassandra.config.DatabaseDescriptor")) match {
    case Failure(ex: ClassNotFoundException) =>
      logger.error(
        s"It looks like you want to use BulkTableWriter but there is no db-all / cassandra-all " +
          s"dependency on the classpath. BulkTableWriter requires one of those dependencies to be added.")
    case _ =>
  }

  private def makeOutputDirectory(path: Option[Path], keyspaceName: String, tableName: String): File = {
    path match {
      case Some(p) => p.toFile
      case None =>
        val tmpDir = System.getProperty("java.io.tmpdir")
        val uuid = UUID.randomUUID()
        new File(tmpDir, s"bulk-write-to-$keyspaceName-$tableName-$uuid")
    }
  }

  def apply[T : RowWriterFactory]
      (connector: CassandraConnector,
       keyspaceName: String,
       tableName: String,
       columnNames: ColumnSelector,
       writeConf: BulkWriteConf): BulkTableWriter[T] = {

    val schema = Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val rowWriter = implicitly[RowWriterFactory[T]]
      .rowWriter(tableDef, columnNames.selectFrom(tableDef))
    val outputDirectory = makeOutputDirectory(writeConf.outputDirectory, keyspaceName, tableName)
    new BulkTableWriter[T](connector, tableDef, rowWriter, outputDirectory,
      deleteSource = writeConf.deleteSource, bufferSizeInMB = writeConf.bufferSizeInMB)
  }

  /** Import `BulkTableWriter._` to enhance your RDDs with bulk saving capability. */
  implicit class BulkSaveRDDFunctions[T: RowWriterFactory](rdd: RDD[T]) {

    /** Writes RDD data to sstables in a local temp directory and
      * then streams the sstables to the Cassandra cluster. The keyspace and table must exist.
      *
      * Depending on the setup this method may or may not be faster than standard `saveToCassandra` call.
      * Compared to `saveToCassandra` call, this method does more work on the client-side. Therefore it
      * uses more memory and I/O on the client, however it puts less stress on the server-side.
      * Use bulk saving if you experience timeouts or server-side OOMs when using `saveToCassandra` method.
      *
      * Make sure your Spark partitions are at least several tens of MBs large,
      * because `bulkSaveToCassandra` will generate at least
      * one sstable per Spark partition.
      */
    def bulkSaveToCassandra(keyspaceName: String,
                            tableName: String,
                            columns: ColumnSelector = AllColumns,
                            writeConf: BulkWriteConf = BulkWriteConf()): Unit = {
      val connector = CassandraConnector(rdd.sparkContext.getConf)
      val writer = BulkTableWriter[T](connector, keyspaceName, tableName, columns, writeConf)
      rdd.sparkContext.runJob(rdd, writer.write _)
    }
  }

}
