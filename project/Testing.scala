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
      else if (test.name.contains("CustomFromDriverSpec")) "customdriverspec"
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
      else if (test.name.contains("CustomFromDriverSpec")) "customdriverspec"
      else if (test.name.contains("CETSpec") || test.name.contains("CETTest")) "cetspec"
      else if (test.name.contains("PSTSpec") || test.name.contains("PSTTest")) "pstspec"
      else if (test.name.contains("Connector")) "connector"
      else "other"
    }

    val groupingFunction = if (false /*parallelTasks == 1*/)
      singleCInstanceGroupingFunction _ else multiCInstanceGroupingFunction _

    val groups = tests.groupBy(groupingFunction).map { case (pkg, testsSeq) =>
      new Group(name = pkg, testsSeq, SubProcess(ForkOptions()))
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
    println(s"Running $parallelTasks Parallel Tasks")
    parallelTasks
  }

}
