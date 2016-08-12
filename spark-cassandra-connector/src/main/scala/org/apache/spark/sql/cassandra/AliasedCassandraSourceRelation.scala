package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.mapper.ColumnMapperConvention._
import com.datastax.spark.connector.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.DataTypeConverter.toStructField
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
  * Decorates [[CassandraSourceRelation]] with read aliasing functionality. Proxies all read-related
  * invocations to given `cassandraSourceRelation` and translates aliased columns to actual C* columns
  * along the way. Back and forth if needed.
  *
  * This class ought to know how to alias columns in all Spark Filters.
  */
private[cassandra] class AliasedCassandraSourceRelation(
  cassandraSourceRelation: CassandraSourceRelation,
  override val sqlContext: SQLContext
) extends BaseRelation
  with InsertableRelation
  with PrunedFilteredScan
  with TableScan
  with Logging {

  private lazy val columnsByAlias = cassandraSourceRelation.tableDef.columns.map(
    col => (underscoreToCamelCase(col.columnName), col)).toMap

  private lazy val aliasByColumnName = columnsByAlias.map({
    case (alias, column) => (column.columnName, alias)
  })

  private def aliasColumnName(f: Filter, alias: String => String): Filter = f match {
    case f: EqualTo => f.copy(attribute = alias(f.attribute))
    case f: EqualNullSafe => f.copy(attribute = alias(f.attribute))
    case f: GreaterThan => f.copy(attribute = alias(f.attribute))
    case f: GreaterThanOrEqual => f.copy(attribute = alias(f.attribute))
    case f: LessThan => f.copy(attribute = alias(f.attribute))
    case f: LessThanOrEqual => f.copy(attribute = alias(f.attribute))
    case f: In => f.copy(attribute = alias(f.attribute))
    case f: IsNull => f.copy(attribute = alias(f.attribute))
    case f: IsNotNull => f.copy(attribute = alias(f.attribute))
    case f: And => And(aliasColumnName(f.left, alias), aliasColumnName(f.right, alias))
    case f: Or => Or(aliasColumnName(f.left, alias), aliasColumnName(f.right, alias))
    case f: Not => Not(aliasColumnName(f.child, alias))
    case f: StringStartsWith => f.copy(attribute = alias(f.attribute))
    case f: StringEndsWith => f.copy(attribute = alias(f.attribute))
    case f: StringContains => f.copy(attribute = alias(f.attribute))
    case _ => throw new IllegalArgumentException(
      s"Don't know how to alias name from the predicate: $f")
  }

  private def alias(name: String): String =
    aliasByColumnName.getOrElse(name, name)

  private def alias(filter: Filter): Filter =
    aliasColumnName(filter, alias)

  private def unalias(name: String): String =
    columnsByAlias.get(name).map(_.columnName).getOrElse(name)

  private def unalias(filter: Filter): Filter =
    aliasColumnName(filter, unalias)

  override def schema: StructType = {
    val columns = columnsByAlias.map { case (alias, column) => column.copy(columnName = alias) }
    StructType(columns.map(toStructField).toSeq)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit =
    cassandraSourceRelation.insert(data, overwrite)

  override def buildScan(): RDD[Row] = cassandraSourceRelation.buildScan()

  override def buildScan(aliasedRequiredColumns: Array[String], aliasedFilters: Array[Filter]): RDD[Row] = {
    val requiredColumns = aliasedRequiredColumns.map(unalias)
    val filters = aliasedFilters.map(unalias)
    cassandraSourceRelation.buildScan(requiredColumns, filters)
  }

  override def unhandledFilters(aliasedFilters: Array[Filter]): Array[Filter] = {
    val filters = aliasedFilters.map(unalias)
    val unhandledFilters = cassandraSourceRelation.unhandledFilters(filters)
    unhandledFilters.map(alias)
  }
}
