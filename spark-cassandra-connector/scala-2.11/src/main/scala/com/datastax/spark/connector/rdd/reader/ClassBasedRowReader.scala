package com.datastax.spark.connector.rdd.reader

import scala.reflect.runtime.universe._

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper

/** Transforms a Cassandra Java driver `Row` into an object of a user provided class, calling the class constructor */
class ClassBasedRowReader[R : TypeTag : ColumnMapper](table: TableDef, skipColumns: Int = 0)
  extends AbstractClassBasedRowReader[R](table, skipColumns) {

  protected[reader] def methodSymbol(name: String): MethodSymbol =
    tpe.decl(TermName(name)).asMethod

}
