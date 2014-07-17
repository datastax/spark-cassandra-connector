/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.commons.configuration.ConfigurationException
import org.apache.spark.streaming.{Seconds, Duration}

import scala.util.Try

/** Companion */
object SparkConnectorSettings {

  def apply(): SparkConnectorSettings =
    apply(ConfigFactory.load)

  def apply(config: Config): SparkConnectorSettings =
    new SparkConnectorSettings(config)
}

private[connector] final class SparkConnectorSettings(val config: Config) {

  private lazy val spark = config.getConfig("spark")
  private lazy val cassandra = spark.getConfig("cassandra")

  lazy val SparkMaster: String = Option(spark.getString("master")) getOrElse withFallbacks("spark.master", "127.0.0.1")

  lazy val SparkPort: Int = Option(spark.getInt("driver.port")) getOrElse withFallbacks("spark.driver.port", 7777)

  lazy val SparkAppName: String = Option(spark.getString("app.name")) getOrElse withFallbacks("spark.app.name", "")

  /* Something odd with Config version conflicts: config.getDuration using `1s` etc. For now: */
  lazy val SparkStreamingBatchDuration: Duration =
    Try(spark.getInt("streaming.batch.duration")).toOption match {
      case None => Seconds(1)
      case Some(n) => Seconds(n)
    }

  lazy val CassandraHost: String = Try(cassandra.getString("connection.host"))
    .toOption getOrElse withFallbacks("spark.cassandra.connection.host", "127.0.0.1")

  lazy val CassandraUserName: String = Try(cassandra.getString("username"))
    .toOption getOrElse withFallbacks("spark.cassandra.username", "cassandra")

  lazy val CassandraPassword: String = Try(cassandra.getString("password"))
    .toOption getOrElse withFallbacks("spark.cassandra.password", "cassandra")

  lazy val CassandraBatchSizeInRows: Option[Int] = {
    val Number = "([0-9]+)".r
    cassandra.getAnyRef("output.batch.size.rows") match {
      case "auto" => None
      case Number(x) => Some(x.toInt)
      case other =>
        throw new ConfigurationException(
          s"Invalid value of spark.cassandra.output.batch.size.rows: $other. Number or 'auto' expected")
    }
  }

  lazy val CassandraBatchSizeInBytes: Long = cassandra.getBytes("output.batch.size.bytes")

  lazy val CassandraWriteParallelismLevel: Int = cassandra.getInt("output.concurrent.writes")

  def withFallbacks[T](javaProperty: String, default: T): T =
    Option(System.getProperty(javaProperty).asInstanceOf[T]) getOrElse default

}
