import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean
import sbt.{ForkOptions, StdoutOutput, TestDefinition}
import sbt.Tests._

object Testing {

  def makeTestGroups(tests: Seq[TestDefinition]): Seq[Group] = {
    // if we have many C* instances and we can run multiple tests in parallel, then group by package name
    // additional groups for auth and ssl is just an optimisation
    def multiCInstanceGroupingFunction(test: TestDefinition): String = {
      if (test.name.toLowerCase.contains("auth")) "auth"
      else if (test.name.toLowerCase.contains("ssl")) "ssl"
      else if (test.name.contains("CustomTableScanMethodSpec")) "customTableScanMethodSpec"
      else if (test.name.contains("CassandraConnectorSourceSpec")) "metricspec"
      else if (test.name.contains("CETSpec") || test.name.contains("CETTest")) "cetspec"
      else if (test.name.contains("PSTSpec") || test.name.contains("PSTTest")) "pstspec"
      else if (test.name.contains("Connector")) "connector"
      else test.name.reverse.dropWhile(_ != '.').reverse
    }

    // if we have a single C* create as little groups as possible to avoid restarting C*
    // the minimum - we need to run REPL and streaming tests in separate processes
    // additional groups for auth and ssl is just an optimisation
    // A new group is made for CustomFromDriverSpec because the ColumnType needs to be
    // Initilized afresh
    def singleCInstanceGroupingFunction(test: TestDefinition): String = {
      val pkgName = test.name.reverse.dropWhile(_ != '.').reverse
      if (test.name.toLowerCase.contains("authenticate")) "auth"
      else if (test.name.toLowerCase.contains("ssl")) "ssl"
      else if (pkgName.contains(".repl")) "repl"
      else if (pkgName.contains(".streaming")) "streaming"
      else if (test.name.contains("CustomTableScanMethodSpec")) "customTableScanMethodSpec"
      else if (test.name.contains("CassandraConnectorSourceSpec")) "metricspec"
      else if (test.name.contains("CETSpec") || test.name.contains("CETTest")) "cetspec"
      else if (test.name.contains("PSTSpec") || test.name.contains("PSTTest")) "pstspec"
      else if (test.name.contains("Connector")) "connector"
      else "other"
    }

    val groupingFunction = if (false /*parallelTasks == 1*/)
      singleCInstanceGroupingFunction _ else multiCInstanceGroupingFunction _

    val groups = tests.groupBy(groupingFunction).map { case (pkg, testsSeq) =>
      new Group(
        name = pkg,
        testsSeq,
        SubProcess(
          ForkOptions()
            .withRunJVMOptions(getCCMJvmOptions.flatten.toVector)
        )
      )
    }.toSeq

    println(s"Tests divided into ${groups.length} groups")

    groups
  }

  lazy val parallelTasks: Int = {
    // Travis has limited quota, so we cannot use many C* instances simultaneously
    val isTravis = sys.props.getOrElse("travis", "false").toBoolean

    val osmxBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val sysMemoryInMB = osmxBean.getTotalPhysicalMemorySize >> 20
    val singleRunRequiredMem = 3 * 1024 + 512
    val parallelTasks = if (isTravis) 1 else Math.max(1, ((sysMemoryInMB - 1550) / singleRunRequiredMem).toInt)
    parallelTasks
  }

  //TODO cleanup with forked CCM
  def getCCMJvmOptions = {
    val ccmCassVersion = sys.env.get("CCM_CASSANDRA_VERSION").map(version => s"-Dccm.version=$version")
    val ccmCassVersion2 = sys.env.get("CCM_CASSANDRA_VERSION").map(version => s"-Dcassandra.version=$version")
    val ccmDse = sys.env.get("CCM_IS_DSE").map(isDSE => s"-Dccm.dse=$isDSE")
    val ccmDse2 = sys.env.get("CCM_IS_DSE").map(isDSE => s"-Ddse=$isDSE")
    val cassandraDirectory = sys.env.get("CCM_INSTALL_DIR").map(dir => s"-Dcassandra.directory=$dir")
    val ccmJava = sys.env.get("CCM_JAVA_HOME").map(dir => s"-Dccm.java.home=$dir")
    val ccmPath = sys.env.get("CCM_JAVA_HOME").map(dir => s"-Dccm.path=$dir/bin")

    val options = Seq(ccmCassVersion, ccmDse, ccmCassVersion2, ccmDse2, cassandraDirectory, ccmJava, ccmPath)
    options
  }

}
