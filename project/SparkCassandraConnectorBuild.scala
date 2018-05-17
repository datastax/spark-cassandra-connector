/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import java.io.File

import sbtsparkpackage.SparkPackagePlugin.autoImport._
import sbt.Keys._
import sbt._

object CassandraSparkBuild extends Build {
  import Settings._
  import sbtassembly.AssemblyPlugin
  import sbtassembly.AssemblyKeys._
  import sbtsparkpackage.SparkPackagePlugin

  val namespace = "spark-cassandra-connector"

  lazy val root = RootProject(
    name = "root",
    dir = file("."),
    settings = rootSettings ++ Seq(Testing.cassandraServerClasspath := { "" }),
    contains = Seq(embedded, connectorDistribution)
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin)

  lazy val cassandraServerProject = Project(
    id = "cassandra-server",
    base = file(namespace),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++= Seq(Artifacts.cassandraServer % "it", Artifacts.airlift),
      Testing.cassandraServerClasspath := {
        (fullClasspath in IntegrationTest).value.map(_.data.getAbsoluteFile).mkString(File.pathSeparator)
      },
      target := target.value / "cassandra-server",
      sourceDirectory := baseDirectory.value / "cassandra-server",
      resourceDirectory := baseDirectory.value / "cassandra-server"
    )
  ) configs IntegrationTest

  lazy val embedded = CrossScalaVersionsProject(
    name = s"$namespace-embedded",
    conf = defaultSettings ++ Seq(
      libraryDependencies
      ++= Dependencies.embedded ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value))
  ).disablePlugins(AssemblyPlugin, SparkPackagePlugin) configs IntegrationTest

  /**
    * Do not included shaded dependencies so they will not be listed in the Pom created for this
    * project.
    *
    * Run the compile from the shaded project since we are no longer including shaded libs
    */
  lazy val connectorDistribution = CrossScalaVersionsProject(
    name = namespace,
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorDistribution ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      //Use the assembly which contains all of the libs not just the shaded ones
      assembly in spPackage := (assembly in shadedConnector).value,
      assembly := (assembly in fullConnector).value,
      //Use the shaded jar as our packageTarget
      packageBin := {
        val shaded = (assembly in shadedConnector).value
        val targetName = target.value
        val expected = target.value / s"$namespace-${version.value}.jar"
        IO.move(shaded, expected)
        val log = streams.value.log
        log.info(s"""Shaded jar moved to $expected""".stripMargin)
        expected
      },
      packagedArtifact in packageBin in Compile := {
        (artifact.value, (assembly in shadedConnector).value)
      },
      sbt.Keys.`package` := packageBin.value)
      ++ Testing.pureCassandraSettings
  ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  /** Because the distribution project has to mark the shaded jars as provided to remove them from
    * the distribution dependencies we provide this additional project to build a fat jar which
    * includes everything. The artifact produced by this project is unshaded while the assembly
    * remains shaded.
    */
  lazy val fullConnector = CrossScalaVersionsProject(
    name = s"$namespace-unshaded",
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorAll
        ++ Dependencies.shaded
        ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      target := target.value / "full"
    )
      ++ Testing.pureCassandraSettings,
    base = Some(namespace)
  ).copy(dependencies = Seq(embedded % "test->test;it->it,test")) configs IntegrationTest


  lazy val shadedConnector = CrossScalaVersionsProject(
    name = s"$namespace-shaded",
    conf = assembledSettings ++ Seq(
      libraryDependencies ++= Dependencies.connectorNonShaded
        ++ Dependencies.shaded
        ++ Seq(
        "org.scala-lang" % "scala-reflect"  % scalaVersion.value,
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test,it"),
      target := target.value / "shaded",
      test in assembly := {},
      publishArtifact in (Compile, packageBin) := false)
      ++ Testing.pureCassandraSettings,
    base = Some(namespace)
  ).copy(dependencies = Seq(embedded % "test->test;it->it,test;")
  ) configs IntegrationTest

  def crossBuildPath(base: sbt.File, v: String): sbt.File = base / s"scala-$v" / "src"

  /* templates */
  def CrossScalaVersionsProject(name: String,
                                conf: Seq[Def.Setting[_]],
                                reliesOn: Seq[ClasspathDep[ProjectReference]] = Seq.empty,
                                base: Option[String] = None) =
    Project(id = name, base = file(base.getOrElse(name)), dependencies = reliesOn, settings = conf ++ Seq(
      unmanagedSourceDirectories in (Compile, packageBin) +=
        crossBuildPath(baseDirectory.value, scalaBinaryVersion.value),
      unmanagedSourceDirectories in (Compile, doc) +=
        crossBuildPath(baseDirectory.value, scalaBinaryVersion.value),
      unmanagedSourceDirectories in Compile +=
        crossBuildPath(baseDirectory.value, scalaBinaryVersion.value)
    ))

  def RootProject(
    name: String,
    dir: sbt.File, settings: =>
    scala.Seq[sbt.Def.Setting[_]] = Seq.empty,
    contains: Seq[ProjectReference]): Project =
      Project(
        id = name,
        base = dir,
        settings = parentSettings ++ settings,
        aggregate = contains)
}

object Artifacts {
  import Versions._

  implicit class Exclude(module: ModuleID) {
    def guavaExclude(): ModuleID =
      module exclude("com.google.guava", "guava")

    /**We will just include netty-all as a dependency **/
    def nettyExclude(): ModuleID = module
      .exclude("io.netty", "netty")
      .exclude("io.netty", "netty-buffer")
      .exclude("io.netty", "netty-codec")
      .exclude("io.netty", "netty-common")
      .exclude("io.netty", "netty-handler")
      .exclude("io.netty", "netty-transport")

    def glassfishExclude(): ModuleID = {
      module
          .exclude("org.glassfish.jersey.containers", "jersey-container-servlet-core")
          .exclude("org.glassfish.jersey.containers", "jersey-container-servlet")
          .exclude("org.glassfish.jersey.core", "jersey-client")
          .exclude("org.glassfish.jersey.core", "jersey-common")
          .exclude("org.glassfish.jersey.core", "jersey-server")
    }

    def sparkCoreExclusions(): ModuleID = module.guavaExclude().nettyExclude().glassfishExclude()
        .exclude("commons-collections", "commons-collections")
        .exclude("commons-beanutils", "commons-beanutils-core")
        .exclude("commons-logging", "commons-logging")
        .exclude("org.apache.curator", "curator-recipes")

    def driverExclusions(): ModuleID = module.guavaExclude().nettyExclude()
        .exclude("io.dropwizard.metrics", "metrics-core")
        .exclude("org.slf4j", "slf4j-api")


    def sparkExclusions(): ModuleID = module.sparkCoreExclusions()
      .exclude("org.apache.spark", s"spark-core_$scalaBinary")

    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")

    def replExclusions: ModuleID = module.guavaExclude()
      .exclude("org.apache.spark", s"spark-bagel_$scalaBinary")
      .exclude("org.apache.spark", s"spark-mllib_$scalaBinary")
      .exclude("org.scala-lang", "scala-compiler")
  }

  val cassandraDriver         = "com.datastax.cassandra"  % "cassandra-driver-core"  % CassandraDriver driverExclusions() // ApacheV2
  val cassandraDriverMapping  = "com.datastax.cassandra"  % "cassandra-driver-mapping"  % CassandraDriver driverExclusions() // ApacheV2
  val commonsBeanUtils        = "commons-beanutils"       % "commons-beanutils"      % CommonsBeanUtils                 exclude("commons-logging", "commons-logging") // ApacheV2
  val config                  = "com.typesafe"            % "config"                 % Config         % "provided"  // ApacheV2
  val guava                   = "com.google.guava"        % "guava"                  % Guava
  val jodaC                   = "org.joda"                % "joda-convert"           % JodaC
  val jodaT                   = "joda-time"               % "joda-time"              % JodaT
  val lzf                     = "com.ning"                % "compress-lzf"           % Lzf            % "provided"
  val netty                   = "io.netty"                % "netty-all"              % Netty
  val slf4jApi                = "org.slf4j"               % "slf4j-api"              % Slf4j          % "provided"  // MIT
  val jsr166e                 = "com.twitter"             % "jsr166e"                % JSR166e                      // Creative Commons
  val airlift                 = "io.airlift"              % "airline"                % Airlift

  val sparkCore           = "org.apache.spark"        %% "spark-core"            % Spark sparkCoreExclusions() // ApacheV2
  val sparkRepl           = "org.apache.spark"        %% "spark-repl"            % Spark sparkExclusions()     // ApacheV2
  val sparkUnsafe         = "org.apache.spark"        %% "spark-unsafe"          % Spark sparkExclusions()     // ApacheV2
  val sparkStreaming      = "org.apache.spark"        %% "spark-streaming"       % Spark sparkExclusions()     // ApacheV2
  val sparkSql            = "org.apache.spark"        %% "spark-sql"             % Spark sparkExclusions()        // ApacheV2
  val sparkCatalyst       = "org.apache.spark"        %% "spark-catalyst"        % Spark sparkExclusions()        // ApacheV2
  val sparkHive           = "org.apache.spark"        %% "spark-hive"            % Spark sparkExclusions()        // ApacheV2

  val cassandraServer     = "org.apache.cassandra"    % "cassandra-all"          % Testing.cassandraTestVersion      logbackExclude()  exclude(org = "org.slf4j", name = "log4j-over-slf4j")  // ApacheV2

  object Metrics {
    val metricsCore       = "com.codahale.metrics"    % "metrics-core"           % CodaHaleMetrics % "provided"
    val metricsJson       = "com.codahale.metrics"    % "metrics-json"           % CodaHaleMetrics % "provided"
  }

  object Jetty {
    val jettyServer       = "org.eclipse.jetty"       % "jetty-server"            % SparkJetty % "provided"
    val jettyServlet      = "org.eclipse.jetty"       % "jetty-servlet"           % SparkJetty % "provided"
  }

  object Embedded {
    val jopt              = "net.sf.jopt-simple"      % "jopt-simple"             % JOpt
    val sparkRepl         = "org.apache.spark"        %% "spark-repl"             % Spark % "provided"    replExclusions    // ApacheV2
    val snappy            = "org.xerial.snappy"       % "snappy-java"             % "1.1.1.7"
    val snakeYaml         = "org.yaml"                % "snakeyaml"               % "1.16"
  }

  object Test {
    val commonsIO         = "commons-io"              % "commons-io"                    % CommonsIO % "test,it"       // ApacheV2
    val scalaCheck        = "org.scalacheck"          %% "scalacheck"                   % ScalaCheck % "test,it"      // BSD
    val scalaMock         = "org.scalamock"           %% "scalamock"                    % ScalaMock % "test,it"       // BSD
    val scalaTest         = "org.scalatest"           %% "scalatest"                    % ScalaTest % "test,it"       // ApacheV2
    val scalactic         = "org.scalactic"           %% "scalactic"                    % Scalactic % "test,it"       // ApacheV2
    val sparkCoreT        = "org.apache.spark"        %% "spark-core"                   % Spark     % "test,it" classifier "tests"
    val sparkStreamingT   = "org.apache.spark"        %% "spark-streaming"              % Spark     % "test,it" classifier "tests"
    val mockito           = "org.mockito"             % "mockito-all"                   % "1.10.19" % "test,it"       // MIT
    val junit             = "junit"                   % "junit"                         % "4.11"    % "test,it"
    val junitInterface    = "com.novocode"            % "junit-interface"               % "0.10"    % "test,it"
    val powerMock         = "org.powermock"           % "powermock-module-junit4"       % "1.6.2"   % "test,it"       // ApacheV2
    val powerMockMockito  = "org.powermock"           % "powermock-api-mockito"         % "1.6.2"   % "test,it"       // ApacheV2
  }
}


object Dependencies {

  import Artifacts._
  import BuildUtil._

  val logging = Seq(slf4jApi)

  val metrics = Seq(Metrics.metricsCore, Metrics.metricsJson)

  val jetty = Seq(Jetty.jettyServer, Jetty.jettyServlet)

  val testKit = Seq(
    sparkRepl % "test,it",
    Test.commonsIO,
    Test.junit,
    Test.junitInterface,
    Test.scalaCheck,
    Test.scalaMock,
    Test.scalaTest,
    Test.scalactic,
    Test.sparkCoreT,
    Test.sparkStreamingT,
    Test.mockito,
    Test.powerMock,
    Test.powerMockMockito
  )

  val cassandra = Seq(cassandraDriver, cassandraDriverMapping)
  val spark = Seq(sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive, sparkUnsafe)

  /**
    * Dependencies which will be shaded in our distribution artifact and not listed on the
    * distribution artifact's dependency list.
    */
  val shaded = Seq(guava, cassandraDriver, cassandraDriverMapping)

  /**
    * This is the full dependency list required to build an assembly with all dependencies
    * required to run the connector not listed as provided except for those which will
    * be on the classpath because of Spark.
    */
  val connectorAll = shaded ++ (testKit ++
    metrics ++
    jetty ++
    logging ++
    spark.map(_ % "provided") ++
    Seq(commonsBeanUtils, jodaC, jodaT, lzf, netty, jsr166e)
    ).map(_ exclude(org = "org.slf4j", name = "log4j-over-slf4j"))

  /**
    * Mark the shaded dependencies as Provided, this removes them from the artifacts to be downloaded
    * by build systems. This will avoid downloading a Cassandra Driver which does not have it's guava
    * references shaded.
    */
  val connectorDistribution = (connectorAll.toSet -- shaded.toSet).toSeq ++ shaded.map(_ % "provided")

  /**
    * When building the shaded jar we want the assembly task to ONLY include the shaded libs, to
    * accomplish this we set all other dependencies as provided.
    */
  val connectorNonShaded = (connectorAll.toSet -- shaded.toSet).toSeq.map { dep =>
    dep.configurations match {
      case Some(conf) => dep
      case _ => dep % "provided"
    }
  }

  val embedded = logging ++ spark ++ cassandra ++ Seq(
    cassandraServer % "it,test" exclude("net.jpountz.lz4", "lz4"),
    Embedded.jopt,
    Embedded.sparkRepl,
    Embedded.snappy,
    Embedded.snakeYaml,
    guava,
    config).map(_ exclude(org = "org.slf4j", name = "log4j-over-slf4j"))

  val documentationMappings = Seq(
    DocumentationMapping(url(s"http://spark.apache.org/docs/${Versions.Spark}/api/scala/"),
      sparkCore, sparkStreaming, sparkSql, sparkCatalyst, sparkHive
    )
  )

}
