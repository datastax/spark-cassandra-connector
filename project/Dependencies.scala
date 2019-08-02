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
      sparkCatalyst,
      sparkHive)
  }

  object DataStax {
    val driverCore = "com.datastax.dse" % "dse-java-driver-core-shaded" % DseJavaDriver
    val driverMapper = "com.datastax.dse" % "dse-java-driver-mapper-runtime" % DseJavaDriver driverCoreExclude()
    val reactiveStream = "org.reactivestreams" % "reactive-streams" % ReactiveStreams
    
    val dependencies = Seq(driverCore, driverMapper, reactiveStream, Temporary.gremlinCore, Temporary.tinkerGraph)
  }

  object Temporary {
    val gremlinCore = "org.apache.tinkerpop" % "gremlin-core" % "3.3.3"  //TODO Remove this when Java Driver includes the correct TP
    val tinkerGraph = "org.apache.tinkerpop" % "tinkergraph-gremlin" % "3.3.3" //TODO Remove this ''
  }

  implicit class Exclude(module: ModuleID) {
    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")
      .exclude("org.slf4j", "log4j-over-slf4j")

    def driverCoreExclude(): ModuleID = module
      .exclude("com.datastax.dse", "dse-java-driver-core") // doesn't shade guava
      .exclude("com.datastax.oss", "java-driver-core") // doesn't shade guava
  }

  object Test {
    val driverMapperProcessor = "com.datastax.dse" % "dse-java-driver-mapper-processor" % DseJavaDriver % "test, it" // Annotation Processor
    val commonsIO         = "commons-io"              %  "commons-io"                   % CommonsIO    % "test,it"       // ApacheV2
    val scalaCheck        = "org.scalacheck"          %% "scalacheck"                   % ScalaCheck   % "test,it"      // BSD
    val scalaTest         = "org.scalatest"           %% "scalatest"                    % ScalaTest    % "test,it"       // ApacheV2
    val sparkCoreT        = "org.apache.spark"        %% "spark-core"                   % ApacheSpark  % "test,it" classifier "tests"
    val sparkStreamingT   = "org.apache.spark"        %% "spark-streaming"              % ApacheSpark  % "test,it" classifier "tests"
    val mockito           = "org.mockito"             %  "mockito-all"                  % Mockito      % "test,it"       // MIT
    val junit             = "junit"                   %  "junit"                        % JUnit        % "test,it"

    val dependencies = Seq(
      driverMapperProcessor,
      scalaCheck,
      scalaTest,
      sparkCoreT,
      sparkStreamingT,
      mockito,
      junit).map(_.logbackExclude())
  }

  // Required for metrics
  object Jetty {
    val jettyServer       = "org.eclipse.jetty"       % "jetty-server"            % SparkJetty % "provided"
    val jettyServlet      = "org.eclipse.jetty"       % "jetty-servlet"           % SparkJetty % "provided"

    val dependencies = Seq(jettyServer, jettyServlet)
  }

  object TestSupport {
    val commonsExec = "org.apache.commons" % "commons-exec" % CommonsExec

    val dependencies = Seq(
      commonsExec,
      Dependencies.DataStax.driverCore)
  }
}