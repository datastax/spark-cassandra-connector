package org.apache.spark.sql.cassandra.api.java

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.api.java.{JavaSQLContext, JavaSchemaRDD}
import org.apache.spark.sql.cassandra.CassandraSQLContext

class JavaCassandraSQLContext(sparkContext: JavaSparkContext) extends JavaSQLContext(sparkContext) {

    override val sqlContext = new CassandraSQLContext(sparkContext)

    /**
      * Executes a query expressed in SQL, returning the result as a JavaSchemaRDD.
      */
    def cql(cqlQuery: String): JavaSchemaRDD =
      new JavaSchemaRDD(sqlContext, sqlContext.parseSql(cqlQuery))
}
