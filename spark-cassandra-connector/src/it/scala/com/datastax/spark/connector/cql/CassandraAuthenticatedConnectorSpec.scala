package com.datastax.spark.connector.cql

import com.datastax.spark.connector.embedded.SparkTemplate._

import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.toDataFrameFunctions

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraSQLContext

class CassandraAuthenticatedConnectorSpec extends SparkCassandraITFlatSpecBase {

   useCassandraConfig(Seq("cassandra-password-auth.yaml.template"))
   useSparkConf(defaultConf)

   // Wait for the default user to be created in Cassandra.
   Thread.sleep(1000)

   val conf = defaultConf
   conf.set(DefaultAuthConfFactory.UserNameParam.name, "cassandra")
   conf.set(DefaultAuthConfFactory.PasswordParam.name, "cassandra")

   "A CassandraConnector" should "authenticate with username and password when using native protocol" in {
      val conn2 = CassandraConnector(conf)
      conn2.withSessionDo { session =>
         assert(session !== null)
         assert(session.isClosed === false)
         assert(session.getCluster.getMetadata.getClusterName != null)
      }
   }

   it should "pick up user and password from SparkConf" in {
      val conf = defaultConf
         .set(DefaultAuthConfFactory.UserNameParam.name, "cassandra")
         .set(DefaultAuthConfFactory.PasswordParam.name, "cassandra")

      // would throw exception if connection unsuccessful
      val conn2 = CassandraConnector(conf)
      conn2.withSessionDo { session => }
   }
   
   "A DataFrame" should "allow to write data with valid authentication" in {

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val conf = defaultConf
         .set(DefaultAuthConfFactory.UserNameParam.name, "cassandra")
         .set(DefaultAuthConfFactory.PasswordParam.name, "cassandra")

      val conn = CassandraConnector(conf)

      val personDF = sc.parallelize(Seq(
         ("Andy", 28, "America"),
         ("Kaushal", 25, "India"),
         ("Desanth", 27, "India"),
         ("Mahendra", 26, "Rajasthan")))
         .toDF("name", "age", "address")

      val options = Map("spark_cassandra_auth_username" -> "cassandra",
         "spark_cassandra_auth_password" -> "cassandra",
         "keyspace" -> ks, "table" -> "authtest")
      createKeyspace(conn.openSession())
      personDF.createCassandraTable(ks, "authtest", Some(Array("address")), Some(Array("age")))(conn)
      personDF.write.format("org.apache.spark.sql.cassandra").options(options).save();
   }

   it should "allow to read data with valid authentication" in {

      val csc = new CassandraSQLContext(sc)

      val options = Map("spark_cassandra_auth_username" -> "cassandra",
         "spark_cassandra_auth_password" -> "cassandra",
         "keyspace" -> ks, "table" -> "authtest")

      val personDF = csc.read.format("org.apache.spark.sql.cassandra").options(options).load();

      personDF.count should be(4)

   }
}
