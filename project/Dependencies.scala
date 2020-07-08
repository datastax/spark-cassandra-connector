import sbt.{ModuleID, _}

object Dependencies
{
  import Versions._

  object Spark {
    val sparkCore = "org.apache.spark" %% "spark-core" % ApacheSpark % "provided" // ApacheV2
    val sparkRepl = "org.apache.spark" %% "spark-repl" % ApacheSpark % "provided" // ApacheV2
    val sparkUnsafe = "org.apache.spark" %% "spark-unsafe" % ApacheSpark % "provided" // ApacheV2
    val sparkStreaming = "org.apache.spark" %% "spark-streaming" % ApacheSpark % "provided" // ApacheV2
    val sparkSql = "org.apache.spark" %% "spark-sql" % ApacheSpark % "provided" // ApacheV2
    val sparkCatalyst = "org.apache.spark" %% "spark-catalyst" % ApacheSpark % "provided" // ApacheV2
    val sparkHive = "org.apache.spark" %% "spark-hive" % ApacheSpark % "provided" // ApacheV2


    val dependencies = Seq(
      sparkCore,
      sparkRepl,
      sparkUnsafe,
      sparkStreaming,
      sparkSql,
      sparkHive,
      sparkCatalyst)
  }

  implicit class Exclude(module: ModuleID) {
    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")
      .exclude("org.slf4j", "log4j-over-slf4j")

    def driverCoreExclude(): ModuleID = module
      .exclude("com.datastax.oss", "java-driver-core") // doesn't shade guava
      .exclude("org.apache.tinkerpop", "*")
      // until SPARK-20075 is fixed we fallback to java workarounds for native calls
      .exclude("com.github.jnr", "jnr-posix")
  }

  object TestCommon {
    val mockito = "org.mockito" % "mockito-all" % Mockito
    val junit = "junit" % "junit" % JUnit
    val junitInterface = "com.novocode" % "junit-interface" % JUnitInterface
    val scalaTest = "org.scalatest" %% "scalatest" % ScalaTest
    val driverMapperProcessor = "com.datastax.oss" % "java-driver-mapper-processor" % DataStaxJavaDriver
  }

  object TestConnector {
    val commonsIO         = "commons-io"              %  "commons-io"                   % CommonsIO    % "test,it"       // ApacheV2
    val scalaCheck        = "org.scalacheck"          %% "scalacheck"                   % ScalaCheck   % "test,it"      // BSD
    val sparkCoreT        = "org.apache.spark"        %% "spark-core"                   % ApacheSpark  % "test,it" classifier "tests"
    val sparkStreamingT   = "org.apache.spark"        %% "spark-streaming"              % ApacheSpark  % "test,it" classifier "tests"
    val solrj             = "org.apache.solr"         %  "solr-solrj"                   % SolrJ      % "test,it"

    val dependencies = Seq(
      scalaCheck,
      sparkCoreT,
      sparkStreamingT,
      solrj,
      TestCommon.driverMapperProcessor % "test,it" driverCoreExclude(),
      TestCommon.scalaTest % "test,it",
      TestCommon.mockito % "test,it",
      TestCommon.junit % "test,it",
      TestCommon.junitInterface % "test,it").map(_.logbackExclude())
  }

  // Required for metrics
  object Jetty {
    val jettyServer       = "org.eclipse.jetty"       % "jetty-server"            % SparkJetty % "provided"
    val jettyServlet      = "org.eclipse.jetty"       % "jetty-servlet"           % SparkJetty % "provided"

    val dependencies = Seq(jettyServer, jettyServlet)
  }

  object Driver {
    val driverCore = "com.datastax.oss" % "java-driver-core-shaded" % DataStaxJavaDriver driverCoreExclude()
    val driverMapper = "com.datastax.oss" % "java-driver-mapper-runtime" % DataStaxJavaDriver driverCoreExclude()

    val commonsLang3 = "org.apache.commons" % "commons-lang3" % Versions.CommonsLang3
    val paranamer = "com.thoughtworks.paranamer" % "paranamer" % Versions.Paranamer

    val dependencies = Seq(driverCore, driverMapper, commonsLang3, paranamer)
  }

  object TestDriver {
    val dependencies = Seq(
      TestCommon.scalaTest % "test",
      TestCommon.mockito % "test",
      TestCommon.junit % "test",
      TestCommon.junitInterface % "test",
      TestCommon.driverMapperProcessor % "test" driverCoreExclude()
    )
  }

  object TestSupport {
    val commonsExec = "org.apache.commons" % "commons-exec" % CommonsExec

    val dependencies = Seq(
      commonsExec,
      Dependencies.Driver.driverCore)
  }
}