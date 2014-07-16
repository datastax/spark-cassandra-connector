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

/** Companion */
object SparkConnectorSettings {

  def apply(): SparkConnectorSettings =
    apply(ConfigFactory.load)

  def apply(config: Config): SparkConnectorSettings =
    new SparkConnectorSettings(config)
}

private[connector] final class SparkConnectorSettings(val config: Config) {

  private lazy val sc = config.getConfig("spark-connector")
  private lazy val spark = sc.getConfig("spark")
  private lazy val cassandra = spark.getConfig("cassandra")

  lazy val SparkMaster: String = spark.getString("master")

  lazy val SparkMasterHost: String = spark.getString("driver-host")

  lazy val SparkDriverPort: Int = spark.getInt("driver-port")

  lazy val SparkAppName: String = spark.getString("app-name")

  lazy val CassandraHost: String = cassandra.getString("host")

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
}
