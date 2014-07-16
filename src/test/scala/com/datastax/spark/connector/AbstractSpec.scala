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

import scala.collection.immutable
import scala.util.Random
import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor.{Actor, ActorRef}
import org.apache.spark.SparkConf
import org.scalatest.{WordSpecLike, Matchers}

trait AbstractSpec extends WordSpecLike with Matchers

/** Extend (and augment) this fixture for the other input stream types by adding abstract class specs */
trait SparkContextFixture {

  implicit val DefaultTimeout = Timeout(5.seconds)

  val actorName = "my-actor"

  val data = immutable.Set("words ", "may ", "count ")

}

object SparkContextFixture {

  val akkaConfig: Config = ConfigFactory.parseString(
    """
      |spark-connector {
      |  spark {
      |    master = "local[12]"
      |    app-name = "Streaming Demo"
      |  }
      |}
      |
      |akka {
      |  loggers = ["akka.testkit.TestEventListener"]
      |  loglevel = "WARNING"
      |  stdout-loglevel = "WARNING"
      |  remote.netty.tcp.port = 0
      |
      |  producer-dispatcher {
      |    type = Dispatcher
      |    executor = "thread-pool-executor"
      |    thread-pool-executor {
      |      core-pool-size-min = 2
      |      core-pool-size-factor = 2.0
      |      core-pool-size-max = 10
      |    }
      |    throughput = 100
      |  }
      |
      |  akka.actor.deployment {
      |    /myactor {
      |      dispatcher = my-dispatcher
      |    }
      |  }
      |}
    """.stripMargin).withFallback(ConfigFactory.load)


  /* WIP */
  val settings = SparkConnectorSettings(SparkContextFixture.akkaConfig)
  import settings._

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraHost)
    .setAppName(SparkAppName)
    .setMaster(SparkMaster)

}

class TestProducer(data: Array[String], to: ActorRef, scale: Int) extends Actor {

  import context.dispatcher

  val rand = new Random()
  var count = 0

  val task = context.system.scheduler.schedule(2.second, 1.millis) {
    to ! makeMessage()
    count += 1
    if (count == scale) self ! "stop"
  }

  def receive: Actor.Receive = {
    case "stop" =>
      task.cancel()
      context stop self
  }

  def makeMessage(): String = {
    val x = rand.nextInt(3)
    data(x) + data(2 - x)
  }
}