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


    val spark = Seq(
      sparkCore,
      sparkRepl,
      sparkUnsafe,
      sparkStreaming,
      sparkSql,
      sparkCatalyst,
      sparkHive)
  }

  object DataStax {
    val driverCore = "com.datastax.dse" % "dse-java-driver-core" % DseJavaDriver nettyExclude()
    val driverMapping = "com.datastax.dse" % "dse-java-driver-mapping" % DseJavaDriver nettyExclude()
    val driverExtras = "com.datastax.dse" % "dse-java-driver-extras" % DseJavaDriver nettyExclude()
    val dataStax = Seq(driverCore, driverMapping, driverExtras)
  }

  object Solr {
    val solrJ = "org.apache.solr" % "solr-solrj" % SolrJ
    val solr = Seq(solrJ)
  }


  implicit class Exclude(module: ModuleID) {
    def logbackExclude(): ModuleID = module
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("ch.qos.logback", "logback-core")
      .exclude("org.slf4j", "log4j-over-slf4j")

    def nettyExclude(): ModuleID = module
      .exclude("io.netty", "netty")
      .exclude("io.netty", "netty-buffer")
      .exclude("io.netty", "netty-codec")
      .exclude("io.netty", "netty-common")
      .exclude("io.netty", "netty-handler")
      .exclude("io.netty", "netty-transport")
  }

  object Test {
    val assertJ           = "org.assertj"             % "assertj-core"                  % AssertJ % "test, it"
    val commonsExec       = "org.apache.commons"      % "commons-exec"                  % CommonsExec % "test, it"
    val commonsIO         = "commons-io"              % "commons-io"                    % CommonsIO % "test,it"       // ApacheV2
    val scalaCheck        = "org.scalacheck"          %% "scalacheck"                   % ScalaCheck % "test,it"      // BSD
    val scalaMock         = "org.scalamock"           %% "scalamock"                    % ScalaMock % "test,it"       // BSD
    val scalaTest         = "org.scalatest"           %% "scalatest"                    % ScalaTest % "test,it"       // ApacheV2
    val scalactic         = "org.scalactic"           %% "scalactic"                    % Scalactic % "test,it"       // ApacheV2
    val sparkCoreT        = "org.apache.spark"        %% "spark-core"                   % ApacheSpark     % "test,it" classifier "tests"
    val sparkStreamingT   = "org.apache.spark"        %% "spark-streaming"              % ApacheSpark     % "test,it" classifier "tests"
    val testNG            = "org.testng"              % "testng"                        % TestNG % "test, it"
    val mockito           = "org.mockito"             % "mockito-all"                   % "1.10.19" % "test,it"       // MIT
    val junit             = "junit"                   % "junit"                         % "4.11"    % "test,it"
    val junitInterface    = "com.novocode"            % "junit-interface"               % "0.10"    % "test,it"

    val testDeps = Seq(
      assertJ,
      commonsExec,
      commonsIO,
      scalaCheck,
      scalaMock,
      scalaTest,
      scalactic,
      sparkCoreT,
      sparkStreamingT,
      testNG,
      mockito,
      junit,
      junitInterface).map(_.logbackExclude)
  }

  // Required for metrics
  object Jetty {
    val jettyServer       = "org.eclipse.jetty"       % "jetty-server"            % SparkJetty % "provided"
    val jettyServlet      = "org.eclipse.jetty"       % "jetty-servlet"           % SparkJetty % "provided"
    val jetty = Seq(jettyServer, jettyServlet)
  }
}