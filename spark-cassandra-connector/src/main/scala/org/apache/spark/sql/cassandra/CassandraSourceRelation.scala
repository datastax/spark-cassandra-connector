package org.apache.spark.sql.cassandra

import scala.reflect.ClassTag

import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.catalyst.expressions.{Literal, AttributeReference, Expression, Attribute}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef}
import com.datastax.spark.connector.rdd.{ReadConf, ValidRDDType}
import com.datastax.spark.connector.writer.{WriteConf, SqlRowWriter}

/**
 * Table scan type. It allows to create a Relation for given table, keyspace, cluster and user
 * defined schema. It also allow to pass [[CassandraConnector]], [[ReadConf]] and [[WriteConf]]
 * explicitly
 */
sealed trait ScanType {

  def makeRelation(
      table: String,
      keyspace: String,
      cluster: Option[String],
      userSpecifiedSchema: Option[StructType],
      connector: CassandraConnector,
      readConf: ReadConf,
      writeConf: WriteConf,
      sqlContext: SQLContext) : BaseRelationImpl
}

/** Base table scan type */
case object BaseScanType extends ScanType  {

  override def makeRelation(
               table: String,
               keyspace: String,
               cluster: Option[String],
               userSpecifiedSchema: Option[StructType],
               connector: CassandraConnector, readConf: ReadConf, writeConf: WriteConf,
               sqlContext: SQLContext) : BaseRelationImpl = {

    new TableScanRelationImpl(
        table,
        keyspace,
        cluster,
        connector,
        readConf,
        writeConf,
        userSpecifiedSchema,
        sqlContext)
  }

}

/** Pruned columns table scan type */
case object PrunedScanType extends ScanType {

  override def makeRelation(
               table: String,
               keyspace: String,
               cluster: Option[String],
               userSpecifiedSchema: Option[StructType],
               connector: CassandraConnector,
               readConf: ReadConf,
               writeConf: WriteConf,
               sqlContext: SQLContext) : BaseRelationImpl = {

    new PrunedScanRelationImpl(
        table,
        keyspace,
        cluster,
        connector,
        readConf,
        writeConf,
        userSpecifiedSchema,
        sqlContext)
  }

}

/** Pruned columns and filtered table scan type */
case object PrunedFilteredScanType extends ScanType {

  override def makeRelation(
               table: String,
               keyspace: String,
               cluster: Option[String],
               userSpecifiedSchema: Option[StructType],
               connector: CassandraConnector,
               readConf: ReadConf,
               writeConf: WriteConf,
               sqlContext: SQLContext) : BaseRelationImpl = {

    new PrunedFilteredScanRelationImpl(
        table,
        keyspace,
        cluster,
        connector,
        readConf,
        writeConf,
        userSpecifiedSchema,
        sqlContext)
  }

}

/** Pruned columns and Catalyst filtered table scan type */
case object CatalystScanType extends ScanType {

  override def makeRelation(
               table: String,
               keyspace: String,
               cluster: Option[String],
               userSpecifiedSchema: Option[StructType],
               connector: CassandraConnector,
               readConf: ReadConf,
               writeConf: WriteConf,
               sqlContext: SQLContext) : BaseRelationImpl = {

    new CatalystScanRelationImpl(
        table,
        keyspace,
        cluster,
        connector,
        readConf,
        writeConf,
        userSpecifiedSchema,
        sqlContext)
  }

}

/**
 *  Base relation implements [[BaseRelation]] and [[InsertableRelation]].
 *  It allows to insert data into Cassandra table. It also provides some
 *  helper methods and variables for sub class to use.
 */
private[cassandra] class BaseRelationImpl(
                         table: String,
                         keyspace: String,
                         cluster: Option[String],
                         connector: CassandraConnector,
                         readConf: ReadConf,
                         writeConf: WriteConf,
                         userSpecifiedSchema: Option[StructType],
                         override val sqlContext: SQLContext) extends BaseRelation
with InsertableRelation with Serializable with Logging {

  protected[this] val tableDef = sqlContext
    .getCassandraSchema(cluster.getOrElse("default"))
    .keyspaceByName(keyspace)
    .tableByName(table)

  protected[this] val columnNameByLowercase = tableDef.allColumns
    .map(c => (c.columnName.toLowerCase, c.columnName)).toMap

  override def schema: StructType = {

    def columnToStructField(
        column: ColumnDef): StructField = {

      StructField(column.columnName, ColumnDataType.catalystDataType(column.columnType, nullable = true))
    }
    userSpecifiedSchema.getOrElse(StructType(tableDef.allColumns.map(columnToStructField)))
  }

  protected[this] val baseRdd = sqlContext.sparkContext
    .cassandraTable[CassandraSQLRow](keyspace, table)(
      connector,
      readConf,
      implicitly[ClassTag[CassandraSQLRow]],
      CassandraSQLRowReader,
      implicitly[ValidRDDType[CassandraSQLRow]])

  //TODO: need add some tests for insert null
  override def insert(
               data: DataFrame,
               overwrite: Boolean): Unit = {

    data.rdd.saveToCassandra(
      keyspace,
      table,
      AllColumns,
      writeConf)(connector, SqlRowWriter.Factory)
  }

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]
}

/**
 * Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[TableScan]]
 * It allows to insert data to and scan the table. It doesn't prune columns or push down any
 * filter to connector. To improve performance, try [[PrunedFilteredScan]], or
 * [[PrunedFilteredScan]], or [[CatalystScan]]
 */
private[cassandra] class TableScanRelationImpl(
                         table: String,
                         keyspace: String,
                         cluster: Option[String],
                         connector: CassandraConnector,
                         readConf: ReadConf,
                         writeConf: WriteConf,
                         userSpecifiedSchema: Option[StructType],
                         override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    table,
    keyspace,
    cluster,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext) with TableScan {

  override def buildScan(): RDD[Row] = super.buildScan()
}

/**
 * Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedScan]]
 * It allows to insert data to and scan the table. It passes the required columns to connector
 * so that only those columns return.
 */
private[cassandra] class PrunedScanRelationImpl(
                         table: String,
                         keyspace: String,
                         cluster: Option[String],
                         connector: CassandraConnector,
                         readConf: ReadConf,
                         writeConf: WriteConf,
                         userSpecifiedSchema: Option[StructType],
                         override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    table,
    keyspace,
    cluster,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with PrunedScan {

  override def buildScan(
               requiredColumns: Array[String]): RDD[Row] = {

    val transformer = new FilterRddTransformer(requiredColumns, columnNameByLowercase, Seq.empty)
    transformer.maybeSelect apply baseRdd
  }
}

/**
 * Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedFilteredScan]]
 * It allows to insert data to and scan the table. Some filters are pushed down to connector,
 * others are combined into a combination filter which is applied to the data returned from connector.
 * [[CatalystScanRelationImpl]] may be more efficient than [[PrunedScanRelationImpl]]
 * In case there's some issue, switch to [[BaseRelation]], or [[PrunedFilteredScan]] or
 * [[CatalystScan]]
 *
 * This is the default Scanner to access Cassandra.
 */
private[cassandra] class PrunedFilteredScanRelationImpl(
                         table: String,
                         keyspace: String,
                         cluster: Option[String],
                         connector: CassandraConnector,
                         readConf: ReadConf,
                         writeConf: WriteConf,
                         userSpecifiedSchema: Option[StructType],
                         override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    table,
    keyspace,
    cluster,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext)
  with PrunedFilteredScan {

  override def buildScan(
               requiredColumns: Array[String],
               filters: Array[Filter]): RDD[Row] = {

    val pushDown = new FilterPushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    val dataTypeMapping: Map[String, (Int, DataType)] = requiredColumns.map(column =>
      (column, (requiredColumns.indexOf(column),
                ColumnDataType.catalystDataType(tableDef.columnByName(column).columnType,
                 nullable = true)
                )
      )
    ).toMap

    def getSchemaData(
        column: String,
        row: Row): (Any, NativeType) = {

      val (index, dataType): (Int, DataType) = dataTypeMapping(column)

      require(dataType.isPrimitive, s"${dataType.typeName} is not supported in filter.")
      (row.get(index), dataType.asInstanceOf[NativeType])
    }

    /** Evaluate filter by column value from the row */
    def translateFilter(
        filter: Filter): Row => Boolean = filter match {

      case EqualTo(column, v) => (a: Row)            => compareColumnValue(column, a, v) == 0
      case LessThan(column, v) => (a: Row)           => compareColumnValue(column, a, v) < 0
      case LessThanOrEqual(column, v) => (a: Row)    => compareColumnValue(column, a, v) <= 0
      case GreaterThan(column, v) => (a: Row)        => compareColumnValue(column, a, v) > 0
      case GreaterThanOrEqual(column, v) => (a: Row) => compareColumnValue(column, a, v) >= 0
      case In(column, values) => (a: Row) => val (value, dataType) = getSchemaData(column, a)
        values.toSet.contains(value)
      //TODO: need add some tests for NULL
      case IsNull(column) => (a: Row) => val (value, dataType) = getSchemaData(column, a)
        value.asInstanceOf[dataType.JvmType] == dataType.asNullable
      case IsNotNull(column) => (a: Row) => val (value, dataType) = getSchemaData(column, a)
        value.asInstanceOf[dataType.JvmType] != dataType.asNullable
      case Not(f) => (a: Row) =>    !translateFilter(f)(a)
      case And(l, r) => (a: Row) => translateFilter(l)(a) && translateFilter(r)(a)
      case Or(l, r) => (a: Row) =>  translateFilter(l)(a) || translateFilter(r)(a)
      case _ => (a: Row) => logWarning(s"Unknown $filter")
        true
    }

    def compareColumnValue(
        column: String,
        row: Row, v: Any): Int = {

      val(value, dataType) = getSchemaData(column, row)
      dataType.ordering.compare(value.asInstanceOf[dataType.JvmType], v.asInstanceOf[dataType.JvmType])
    }
    // a filter combining all other filters
    val translators = preservedFilters.map(translateFilter)
    def rowFilter(row: Row): Boolean = translators.forall(_(row))

    val transformer = new FilterRddTransformer(requiredColumns, columnNameByLowercase, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }
}


/**
 * Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[CatalystScan]]
 * It allows to insert data to and scan the table. It gets filter predicates directly from Catalyst planner
 * Some predicates are pushed down to connector, others are combined into a combination filter which
 * is applied to the data returned from connector. It may be more efficient than [[PrunedScanRelationImpl]]
 * because it uses internal Catalyst predicates eval methods to construct the combined filter and it hooks
 * directly to Catalyst query planner. But in case there's some issue, switch to [[PrunedFilteredScan]]
 */
private[cassandra] class CatalystScanRelationImpl(
                         table: String,
                         keyspace: String,
                         cluster: Option[String],
                         connector: CassandraConnector,
                         readConf: ReadConf,
                         writeConf: WriteConf,
                         userSpecifiedSchema: Option[StructType],
                         override val sqlContext: SQLContext)
  extends BaseRelationImpl(
    table,
    keyspace,
    cluster,
    connector,
    readConf,
    writeConf,
    userSpecifiedSchema,
    sqlContext) with CatalystScan {

  override def buildScan(
               requiredColumns: Seq[Attribute],
               filters: Seq[Expression]): RDD[Row] = {

    val pushDown = new PredicatePushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    val dataTypeMapping: Map[String, (Int, DataType)] = requiredColumns.map(attribute =>
      (attribute.name,  (requiredColumns.indexOf(attribute),
                          ColumnDataType.catalystDataType(tableDef.columnByName(attribute.name).columnType,
                          nullable = true)
                        )
      )
    ).toMap

    def getSchemaData(
        column: String,
        row: Row): (Any, NativeType) = {

      val (index, dataType): (Int, DataType) = dataTypeMapping(column)

      require(dataType.isPrimitive, s"${dataType.typeName} is not supported in filter.")
      (row.get(index), dataType.asInstanceOf[NativeType])
    }

    def rowFilter(row: Row): Boolean = {

      val evalAttributeReference: PartialFunction[Expression, Expression] = {
        case AttributeReference(name, _, _, _) => val (value, dataType) = getSchemaData(name, row)
          Literal(value, dataType)
        case e: Expression => e
      }

      val translators = preservedFilters.map(_.transform(evalAttributeReference))
      !translators.map(_.eval(row).asInstanceOf[Boolean]).contains(false)
    }

    val transformer = new CatalystRddTransformer(requiredColumns, columnNameByLowercase, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }

}