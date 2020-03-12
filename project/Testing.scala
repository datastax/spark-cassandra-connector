import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean
import sbt.Tests._
import sbt.{ForkOptions, TestDefinition}

object Testing {

  private def interfacesImplementingFixture(c: Class[_], fixture: Class[_]): Seq[Class[_]] = {
    c.getInterfaces.toSeq.filter(i => i != fixture && fixture.isAssignableFrom(i)) ++
      c.getInterfaces.flatMap(interfacesImplementingFixture(_, fixture)) ++
      Option(c.getSuperclass).map(x => interfacesImplementingFixture(x, fixture)).getOrElse(Seq())
  }

  def testsWithFixtures(cl: ClassLoader, tests: Seq[TestDefinition]): Map[TestDefinition, Seq[String]] = {
    val fixture = cl.loadClass("com.datastax.spark.connector.cluster.Fixture")
    tests.map { testDefinition =>
      val c = cl.loadClass(testDefinition.name)
      (testDefinition, interfacesImplementingFixture(c, fixture).map(_.getSimpleName).distinct.sorted)
    }.toMap
  }

  def makeTestGroups(testsWithFixtures: Map[TestDefinition, Seq[String]]): Seq[Group] = {
    val (separateJVMTests, groupedTests) = testsWithFixtures
      .partition { case (_, fixtures) => fixtures.contains("SeparateJVM") }

    val testsByGroupName = groupedTests.groupBy(_._2).map { case (fixtures, tests) => (fixtures.mkString("-"), tests.keys) } ++
      separateJVMTests.zipWithIndex.map { case ((test, fixtures), i) => ((fixtures ++ i.toString).mkString("-"), Seq(test)) }

    println(s"All existing tests divided into ${testsByGroupName.size} groups:")
    testsByGroupName.toSeq
      .sortBy(-_._2.size) // biggest groups are executed first
      .zipWithIndex
      .map { case ((groupName, tests), i) =>
        println(s"[T$i] $groupName: ${tests.size} test(s)")
        val envVars = Map("CCM_IP_PREFIX" -> s"127.$i.", "TEST_GROUP_NO" -> i.toString) ++ sys.env +
          ("_JAVA_OPTIONS" -> "-ea") // enable java assertions
        Group(groupName, tests.toSeq, SubProcess(
          ForkOptions()
            .withEnvVars(envVars)
            .withRunJVMOptions(getCCMJvmOptions.flatten.toVector)))
      }
  }

  def getCCMJvmOptions = {
    val ccmCassVersion = sys.env.get("CCM_CASSANDRA_VERSION").map(version => s"-Dccm.version=$version")
    val ccmCassVersion2 = sys.env.get("CCM_CASSANDRA_VERSION").map(version => s"-Dcassandra.version=$version")
    val dseInVersion = if (sys.env.get("CCM_CASSANDRA_VERSION").contains("dse")) Some(true) else None
    val ccmDse = sys.env.get("CCM_IS_DSE").orElse(dseInVersion).map(isDSE => s"-Dccm.dse=$isDSE")
    val ccmDse2 = sys.env.get("CCM_IS_DSE").orElse(dseInVersion).map(isDSE => s"-Ddse=$isDSE")
    val cassandraDirectory = sys.env.get("CCM_INSTALL_DIR").map(dir => s"-Dcassandra.directory=$dir")
    val ccmJava = sys.env.get("CCM_JAVA_HOME").map(dir => s"-Dccm.java.home=$dir")
    val ccmPath = sys.env.get("CCM_JAVA_HOME").map(dir => s"-Dccm.path=$dir/bin")
    val options = Seq(ccmCassVersion, ccmDse, ccmCassVersion2, ccmDse2, cassandraDirectory, ccmJava, ccmPath)
    options
  }

  val MaxParallel = 10

  lazy val parallelTasks: Int = {
    val parallelTasks = sys.env.get("TEST_PARALLEL_TASKS").map(_.toInt).getOrElse {
      val osmxBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
      val sysMemoryInMB = osmxBean.getTotalPhysicalMemorySize >> 20
      val singleRunRequiredMem = 1024 + 1536 // ForkMain + DseModule
      val sbt = 1550
      Math.min(Math.max(1, ((sysMemoryInMB - sbt) / singleRunRequiredMem).toInt), MaxParallel)
    }
    println(s"Running $parallelTasks Parallel Tasks")
    parallelTasks
  }

}
