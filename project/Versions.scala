import scala.util.Properties

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
object Versions {

  val crossScala = Seq("2.11.7", "2.10.5")

  /* Leverages optional Spark 'scala-2.11' profile optionally set by the user via -Dscala-2.11=true if enabled */
  lazy val scalaVersion = sys.props.get("scala-2.11") match {
    case Some(is) if is.nonEmpty && is.toBoolean => crossScala.head
    case crossBuildFor                           => crossScala.last
  }

  /* For `scalaBinaryVersion.value outside an sbt task. */
  lazy val scalaBinary = scalaVersion.dropRight(2)

  val Akka            = "2.3.4"
  val Cassandra       = "2.2.2"
  val CassandraDriver = "3.0.0-alpha4"
  val CommonsIO       = "2.4"
  val CommonsLang3    = "3.3.2"
  val Config          = "1.2.1"
  val Guava           = "16.0.1"
  val JDK             = "1.7"
  val JodaC           = "1.2"
  val JodaT           = "2.3"
  val JOpt            = "3.2"
  val Kafka           = "0.8.2.1"
  val Kafka210        = "0.8.1.1"
  val Lzf             = "0.8.4"
  val CodaHaleMetrics = "3.0.2"
  val ScalaMock       = "3.2"
  val ScalaTest       = "2.2.2"
  val Scalactic       = "2.2.2"
  val Slf4j           = "1.6.1"//1.7.7"

  // Spark version can be specified as:
  // - regular version which is present in some public Maven repository
  // - a release tag in https://github.com/apache/spark
  // - one of main branches, like master or branch-x.y, followed by "-SNAPSHOT" suffix
  // The last two cases trigger the build to clone the given revision of Spark from GitHub, build it
  // and install in a local Maven repository. This is all done automatically, however it will work
  // only on Unix/OSX operating system. Windows users have to build and install Spark manually if the
  // desired version is not yet published into a public Maven repository.
  val Spark           = "1.5.1"
  val SparkJetty      = "8.1.14.v20131031"
  val JSR166e         = "1.1.0"
  val Airlift         = "0.6"

  val hint = (binary: String) => if (binary == "2.10") "[To build against Scala 2.11 use '-Dscala-2.11=true']" else ""

  val status = (versionInReapply: String, binaryInReapply: String) =>
    println(s"""
        |  Scala: $versionInReapply ${hint(binaryInReapply)}
        |  Scala Binary: $binaryInReapply
        |  Java: target=$JDK user=${Properties.javaVersion}
        """.stripMargin)
}
