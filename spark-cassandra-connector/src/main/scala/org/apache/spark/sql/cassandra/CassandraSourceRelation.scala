package org.apache.spark.sql.cassandra

import java.io.IOException

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef}
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLRow.CassandraSQLRowReader
import org.apache.spark.sql.catalyst.expressions.{Literal, AttributeReference, Expression, Attribute}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.reflect.ClassTag

/** Create Cassandra data source relation based on [[ScanType]]*/
case class CassandraSourceRelation(table: String, keyspace: String, scanType: ScanType, cluster: Option[String],
                                   userSpecifiedSchema: Option[StructType], sqlContext: SQLContext) {
  def apply(table: String, keyspace: String, scanType: ScanType, cluster: Option[String],
            userSpecifiedSchema: Option[StructType], sqlContext: SQLContext) = scanType.makeRelation(
    table, keyspace, cluster, userSpecifiedSchema, sqlContext)
}

/** Base table scan relation*/
case object BaseScanType extends ScanType  {
  override def makeRelation(table: String, keyspace: String, cluster: Option[String],
                            userSpecifiedSchema: Option[StructType],sqlContext: SQLContext) =
    new TableScanRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
}

/** Pruned columns table scan relation*/
case object PrunedScanType extends ScanType {
  override def makeRelation(table: String, keyspace: String, cluster: Option[String],
                            userSpecifiedSchema: Option[StructType],sqlContext: SQLContext) =
    new PrunedScanRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
}

/** Pruned columns and filtered table scan relation*/
case object PrunedFilteredScanType extends ScanType {
  override def makeRelation(table: String, keyspace: String, cluster: Option[String],
                            userSpecifiedSchema: Option[StructType],sqlContext: SQLContext) =
    new PrunedFilteredScanRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
}

/** Pruned columns and Catalyst filtered table scan relation*/
case object CatalystScanType extends ScanType {
  override def makeRelation(table: String, keyspace: String, cluster: Option[String],
                            userSpecifiedSchema: Option[StructType],sqlContext: SQLContext) =
    new CatalystScanRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlContext)
}

/** Table scan type */
sealed trait ScanType {
  def makeRelation(table: String, keyspace: String, cluster: Option[String],
                   userSpecifiedSchema: Option[StructType], sqlContext: SQLContext) : BaseRelationImpl
}

/** Base relation implements [[BaseRelation]] and [[InsertableRelation]] */
private[cassandra] class BaseRelationImpl(table: String, keyspace: String, cluster: Option[String],
                                          userSpecifiedSchema: Option[StructType], override val sqlContext: SQLContext)
  extends BaseRelation with InsertableRelation with Serializable with Logging {

  protected[this] val tableDef = sqlContext
    .schemas.get(cluster.getOrElse("default"))
    .keyspaceByName.getOrElse(keyspace, throw new IOException(s"Keyspace not found: $keyspace"))
    .tableByName.getOrElse(table, throw new IOException(s"Table not found: $keyspace.$table"))

  protected[this] val columnNameByLowercase = tableDef.allColumns
    .map(c => (c.columnName.toLowerCase, c.columnName)).toMap

  override def schema: StructType = {
    def columnToStructField(column: ColumnDef): StructField = {
      StructField(column.columnName, ColumnDataType.catalystDataType(column.columnType, nullable = true))
    }
    if (userSpecifiedSchema.isDefined){
      userSpecifiedSchema.get
    } else {
      StructType(tableDef.allColumns.map(columnToStructField))
    }
  }

  protected[this] val baseRdd = sqlContext
    .sparkContext
    .cassandraTable[CassandraSQLRow](keyspace, table)(
      new CassandraConnector(sqlContext.getCassandraConnConf(cluster)),
      sqlContext.getReadConf(keyspace, table, cluster),
      implicitly[ClassTag[CassandraSQLRow]],
      CassandraSQLRowReader,
      implicitly[ValidRDDType[CassandraSQLRow]])

  private[this] val writeConf = sqlContext.getWriteConf(keyspace, table, cluster)

  //TODO: need add some tests for insert null
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.rdd.saveToCassandra(keyspace, table, AllColumns, writeConf)(
      new CassandraConnector(sqlContext.getCassandraConnConf(cluster)), SqlRowWriter.Factory)
  }

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]
}

/** Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[TableScan]] */
private[cassandra] class TableScanRelationImpl(table: String, keyspace: String, cluster: Option[String],
                                               userSpecifiedSchema: Option[StructType], sqlcontext: SQLContext)
  extends BaseRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlcontext) with TableScan {
  override def buildScan(): RDD[Row] = super.buildScan()
}

/** Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedScan]] */
private[cassandra] class PrunedScanRelationImpl(table: String, keyspace: String, cluster: Option[String],
                                                userSpecifiedSchema: Option[StructType], sqlcontext: SQLContext)
  extends BaseRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlcontext) with PrunedScan {
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val transformer = new RddFilterSelectPdTrf(requiredColumns, columnNameByLowercase, Seq.empty)
    transformer.maybeSelect apply baseRdd
  }
}

/** Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[PrunedFilteredScan]] */
private[cassandra] class PrunedFilteredScanRelationImpl(table: String, keyspace: String, cluster: Option[String],
                                                        userSpecifiedSchema: Option[StructType], sqlcontext: SQLContext)
  extends BaseRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlcontext) with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val pushDown = new FilterPushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    val dataTypeMapping: Map[String, (Int, DataType)] = requiredColumns.map(column =>
      (column, (requiredColumns.indexOf(column),
        ColumnDataType.catalystDataType(tableDef.columnByName.get(column).
          getOrElse(throw new RuntimeException(s"Can't find column $column in $table")).columnType,
          nullable = true)))).toMap

    def getSchemaData(column: String, row: Row): (Any, NativeType) = {
      val (index, dataType): (Int, DataType) = dataTypeMapping.get(column).getOrElse(
        throw new IOException(s"Can't find column $column in $table"))
      require(dataType.isPrimitive, s"${dataType.typeName} is not supported in filter.")
      (row.get(index), dataType.asInstanceOf[NativeType])
    }

    /** Evaluate filter by column value from the row */
    def translateFilter(filter: Filter): Row => Boolean = filter match {
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

    def compareColumnValue(column: String, row: Row, v: Any): Int = {
      val(value, dataType) = getSchemaData(column, row)
      dataType.ordering.compare(value.asInstanceOf[dataType.JvmType], v.asInstanceOf[dataType.JvmType])
    }
    // a filter combining all other filters
    val translators = preservedFilters.map(translateFilter)
    def rowFilter(row: Row): Boolean = translators.forall(_(row))

    val transformer = new RddFilterSelectPdTrf(requiredColumns, columnNameByLowercase, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }
}


/** Table scan relation implements [[BaseRelation]], [[InsertableRelation]] and [[CatalystScan]] */
private[cassandra] class CatalystScanRelationImpl(table: String, keyspace: String, cluster: Option[String],
                                                  userSpecifiedSchema: Option[StructType], sqlcontext: SQLContext)
  extends BaseRelationImpl(table, keyspace, cluster, userSpecifiedSchema, sqlcontext) with CatalystScan {
  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    val pushDown = new PredicatePushDown(filters, tableDef)
    val pushdownFilters = pushDown.toPushDown
    val preservedFilters = pushDown.toPreserve

    logInfo(s"pushdown filters: ${pushdownFilters.toString()}")

    val dataTypeMapping: Map[String, (Int, DataType)] = requiredColumns.map(attribute =>
      (attribute.name,  (requiredColumns.indexOf(attribute),
        ColumnDataType.catalystDataType(tableDef.columnByName.get(attribute.name).
          getOrElse(throw new RuntimeException(s"Can't find column ${attribute.name} in the $table")).columnType,
          nullable = true)))).toMap

    def getSchemaData(column: String, row: Row): (Any, NativeType) = {
      val (index, dataType): (Int, DataType) = dataTypeMapping.get(column).getOrElse(
        throw new IOException(s"Can't find column $column in the $table"))
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
      !translators.map(_.eval(row)).contains(false)
    }

    val transformer = new RddPredicateSelectPdTrf(requiredColumns, columnNameByLowercase, pushdownFilters)
    transformer.transform(baseRdd).filter(rowFilter)
  }

}