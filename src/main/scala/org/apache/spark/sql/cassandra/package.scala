package org.apache.spark.sql

import scala.language.implicitConversions

import com.datastax.spark.connector.util.ConfigParameter

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

  implicit class DataFrameWriterWrapper[T](val dfWriter: DataFrameWriter[T]) extends AnyVal {
    /** Sets the format used to access Cassandra through Connector */
    def cassandraFormat: DataFrameWriter[T] = {
      dfWriter.format("org.apache.spark.sql.cassandra")
    }

    /** Sets the format used to access Cassandra through Connector and configure a path to Cassandra table. */
    def cassandraFormat(
        table: String,
        keyspace: String,
        cluster: String = CassandraSourceRelation.defaultClusterName,
        pushdownEnable: Boolean = true): DataFrameWriter[T] = {

      cassandraFormat.options(cassandraOptions(table, keyspace, cluster, pushdownEnable))
    }
  }

  @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
  implicit class CassandraSQLContextFunctions(val sqlContext: SQLContext) extends AnyVal {

    import org.apache.spark.sql.cassandra.CassandraSQLContextParams._

    /** Set current used cluster name */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCluster(cluster: String): SQLContext = {
      sqlContext.setConf(SqlClusterParam.name, cluster)
      sqlContext
    }

    /** Get current used cluster name */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def getCluster: String = sqlContext.getConf(SqlClusterParam.name, SqlClusterParam.default)

    /** Set the Spark Cassandra Connector configuration parameters */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCassandraConf(options: Map[String, String]): SQLContext = {
      setCassandraConf(SqlClusterParam.default, options)
      sqlContext
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given cluster */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCassandraConf(
        cluster: String,
        options: Map[String, String]): SQLContext = {
      checkOptions(options)
      for ((k, v) <- options) sqlContext.setConf(s"$cluster/$k", v)
      sqlContext
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given keyspace in a given cluster */
    @deprecated("Use SparkSession instead of SQLContext", "2.0.0")
    def setCassandraConf(
        cluster: String,
        keyspace: String,
        options: Map[String, String]): SQLContext = {
      checkOptions(options)
      for ((k, v) <- options) sqlContext.setConf(s"$cluster:$keyspace/$k", v)
      sqlContext
    }
  }

  implicit class CassandraSparkSessionFunctions(val sparkSession: SparkSession) extends AnyVal {

    import org.apache.spark.sql.cassandra.CassandraSQLContextParams._

    /** Set current used cluster name */
    def setCluster(cluster: String): SparkSession = {
      sparkSession.conf.set(SqlClusterParam.name, cluster)
      sparkSession
    }

    /** Get current used cluster name */
    def getCluster: String = sparkSession.conf.get(SqlClusterParam.name, SqlClusterParam.default)

    /** Set the Spark Cassandra Connector configuration parameters */
    def setCassandraConf(options: Map[String, String]): SparkSession = {
      setCassandraConf(SqlClusterParam.default, options)
      sparkSession
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given cluster */
    def setCassandraConf(
        cluster: String,
        options: Map[String, String]): SparkSession = {
      checkOptions(options)
      for ((k, v) <- options) sparkSession.conf.set(s"$cluster/$k", v)
      sparkSession
    }

    /** Set the Spark Cassandra Connector configuration parameters which will be used when accessing
      * a given keyspace in a given cluster */
    def setCassandraConf(
        cluster: String,
        keyspace: String,
        options: Map[String, String]): SparkSession = {
      checkOptions(options)
      for ((k, v) <- options) sparkSession.conf.set(s"$cluster:$keyspace/$k", v)
      sparkSession
    }
  }

  object CassandraSQLContextParams {
    // Should use general used database than Cassandra specific keyspace?
    // Other source tables don't have keyspace concept. We should make
    // an effort to set CassandraSQLContext a more database like to join
    // tables from other sources. Keyspace is equivalent to database in SQL world
    val ReferenceSection = "Cassandra SQL Context Options"

    val SqlClusterParam = ConfigParameter[String](
      name = "spark.cassandra.sql.cluster",
      section = ReferenceSection,
      default = "default",
      description = "Sets the default Cluster to inherit configuration from")

    val Properties = Seq(SqlClusterParam)

    private[cassandra] def checkOptions(options: Map[String, String]): Unit = {
      options.keySet.foreach { name =>
        require(DefaultSource.confProperties.contains(name),
          s"Unrelated parameter. You can only set the following parameters: ${DefaultSource.confProperties.mkString(", ")}")
      }
    }
  }

}
