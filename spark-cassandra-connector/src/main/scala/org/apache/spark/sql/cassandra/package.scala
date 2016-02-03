package org.apache.spark.sql

package object cassandra {

  /** A data frame format used to access Cassandra through Connector */
  val CassandraFormat = "org.apache.spark.sql.cassandra"

  /** Returns a map of options which configure the path to Cassandra table as well as whether pushdown is enabled
    * or not */
  def cassandraOptions(
      table: String,
      keyspace: String,
      cluster: String = CassandraSourceRelation.defaultClusterName,
      pushdownEnable: Boolean = true): Map[String, String] =
    Map(
      DefaultSource.CassandraDataSourceClusterNameProperty -> cluster,
      DefaultSource.CassandraDataSourceKeyspaceNameProperty -> keyspace,
      DefaultSource.CassandraDataSourceTableNameProperty -> table,
      DefaultSource.CassandraDataSourcePushdownEnableProperty -> pushdownEnable.toString)

  implicit class DataFrameReaderWrapper(val dfReader: DataFrameReader) extends AnyVal {
    /** Sets the format used to access Cassandra through Connector */
    def cassandraFormat: DataFrameReader = {
      dfReader.format("org.apache.spark.sql.cassandra")
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def cassandraFormat(
        table: String,
        keyspace: String,
        cluster: String = CassandraSourceRelation.defaultClusterName,
        pushdownEnable: Boolean = true): DataFrameReader = {

      cassandraFormat.options(cassandraOptions(table, keyspace, cluster, pushdownEnable))
    }
  }

  implicit class DataFrameWriterWrapper(val dfWriter: DataFrameWriter) extends AnyVal {
    /** Sets the format used to access Cassandra through Connector */
    def cassandraFormat: DataFrameWriter = {
      dfWriter.format("org.apache.spark.sql.cassandra")
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def cassandraFormat(
        table: String,
        keyspace: String,
        cluster: String = CassandraSourceRelation.defaultClusterName,
        pushdownEnable: Boolean = true): DataFrameWriter = {

      cassandraFormat.options(cassandraOptions(table, keyspace, cluster, pushdownEnable))
    }
  }

}
