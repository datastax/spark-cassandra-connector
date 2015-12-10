import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path, Paths, _}

import sbt.{IO, _}

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.xml.{Elem, XML}

object SparkInstaller {

  private val attempts = TrieMap[(String, String), Unit]()

  private def isGitVersion(providedVersion: String): Boolean = {
    val GitTagPattern = """v[0-9].+""".r
    val GitBranchPattern = """(master|branch).*\-SNAPSHOT""".r

    providedVersion match {
      case GitTagPattern() => true
      case GitBranchPattern(_) => true
      case _ => false
    }
  }

  private object StdProcessLogger extends sbt.ProcessLogger {
    override def info(s: => String): Unit =
      if (s.startsWith("\r")) System.out.println("RRR " + s)
      else if (s.endsWith("\r")) System.out.println(s + "RRR")
      else System.out.println(s)

    override def error(s: => String): Unit =
      if (s.startsWith("\r")) System.err.println("RRR " + s)
      else if (s.endsWith("\r")) System.err.println(s + "RRR")
      else System.err.println(s)

    override def buffer[T](f: => T): T = f
  }

  private def isInstalled(version: String, scalaVersion: String): Boolean = {
    val mavenRepoPath = Paths.get(new URL(BuildUtil.mavenLocalResolver.root).toURI)
    val sparkCoreJar = mavenRepoPath.resolve("org").resolve("apache").resolve("spark")
      .resolve(s"spark-core_$scalaVersion").resolve(version)
      .resolve(s"spark-core_$scalaVersion-$version.jar")

    Files.isRegularFile(sparkCoreJar)
  }

  private def cloneSpark(version: String, destination: Path, force: Boolean): Unit = {
    val isSnapshot = version.endsWith("-SNAPSHOT")
    val baseVersion = if (isSnapshot) version.dropRight("-SNAPSHOT".length) else version
    if (!Files.exists(destination.resolve("pom.xml")) || force || isSnapshot) {
      IO.delete(destination.toFile)
      val dir = destination.toAbsolutePath.toString
      val repo = "https://github.com/apache/spark.git"
      val result = s"git clone --branch $baseVersion --depth 1 $repo $dir" ! StdProcessLogger
      if (result != 0)
        throw new RuntimeException("Failed to fetch Spark source code")
    }
  }

  private def replaceVersionInPoms(rootDir: Path, newVersion: String): Unit = {
    def replaceVersionInPomXml(xml: Elem, newVersion: String): Elem = {
      import scala.xml.Node

      def replaceVersion(nodes: Seq[Node]): Seq[Node] =
        for (node <- nodes) yield node match {
          case <version>{_}</version> =>
            <version>{newVersion}</version>
          case parent @ <parent>{nodes@_*}</parent> if (parent \ "groupId").text == "org.apache.spark" =>
            <parent>{replaceVersion(nodes)}</parent>
          case other => other
        }

      xml.copy(child = replaceVersion(xml.child))
    }

    def replaceVersionInPom(file: Path, newVersion: String): Unit = {
      val pom = XML.loadFile(file.toFile)
      val updatedPom = replaceVersionInPomXml(pom, newVersion)
      XML.save(file.toAbsolutePath.toString, updatedPom, "UTF-8")
    }

    Files.walkFileTree(rootDir, Set.empty[FileVisitOption], 100, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        if (file.getFileName.toString == "pom.xml")
          replaceVersionInPom(file, newVersion)

        FileVisitResult.CONTINUE
      }

      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = {
        if (Files.isHidden(dir) || dir.getFileName.toString.startsWith("."))
          FileVisitResult.SKIP_SUBTREE
        else FileVisitResult.CONTINUE
      }
    })
  }

  private def installSpark(rootDir: Path, scalaVersion: String): Unit = {
    val `scala_2.11` = scalaVersion != "2.10"

    if (`scala_2.11`) {
      val cmd = List(
        Paths.get("dev").resolve("change-scala-version.sh").toString,
        "2.11"
      )
      val result = sbt.Process(cmd, rootDir.toFile) ! StdProcessLogger
      if (result != 0)
        throw new RuntimeException("Failed to change Scala version")
    }

    val cmd = List(
      Paths.get("build").resolve("mvn").toString,
      "--force",
      "--batch-mode",
      "-DskipTests"
    ) ::: (if (`scala_2.11`) List("-Dscala-2.11") else Nil) :::
      "install" :: Nil

    val result = sbt.Process(cmd, rootDir.toFile, "AMPLAB_JENKINS" -> "1") ! StdProcessLogger
    if (result != 0)
      throw new RuntimeException("Failed to install Spark")
  }

  private lazy val isWindows = sys.props("os.name").toLowerCase().contains("windows")

  private def getAndInstallSpark(version: String, scalaVersion: String, force: Boolean, rootDir: Path): Unit = {
    if ((force || !isInstalled(version, scalaVersion)) && !attempts.contains(version -> scalaVersion)) {
      attempts.putIfAbsent(version -> scalaVersion, {})
      if (isWindows) {
        println(s"""
          Cannot automatically fetch and install Spark on Windows. You need to do this on your own.
        """)
        throw new RuntimeException("Cannot install Spark")
      }

      val sparkDir = rootDir.resolve("target").resolve("spark-builds").resolve(version)
      if (!Files.isDirectory(sparkDir)) {
        Files.createDirectories(sparkDir)
      }

      try {
        println(s"Cloning Spark $version to $sparkDir...")
        cloneSpark(version, sparkDir, force)

        println("Replacing version in POM files...")
        replaceVersionInPoms(sparkDir, version)

        println("Building and installing Spark to local Maven repository...")
        installSpark(sparkDir, scalaVersion)

        println(s"Spark $version has been installed.")
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          throw ex;
      }
    } else {
      println(s"Spark $version for Scala $scalaVersion detected.")
    }
  }

  private lazy val force = sys.props.get("spark.forceInstall").exists(_.toLowerCase == "true")

  def apply(scalaVersion: String, dir: Path): Unit = {
    val version = Versions.Spark
    if (isGitVersion(version) && !Versions.doNotInstallSpark) {
      getAndInstallSpark(version, scalaVersion, force, dir)
    }
  }

}