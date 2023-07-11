import java.io.File

import sbt.Path
import sbt._
import sbt.librarymanagement.Resolver
import sbt.librarymanagement.ivy.Credentials

import scala.sys.process._

object Publishing extends sbt.librarymanagement.DependencyBuilders {

  val Version: String = {
    sys.props.get("publish.version").orElse(sys.env.get("PUBLISH_VERSION")).getOrElse("git describe --tags" !!).stripLineEnd.stripPrefix("v")
  }

  val altReleaseDeploymentRepository = sys.props.get("publish.repository.name")
  val altReleaseDeploymentLocation = sys.props.get("publish.repository.location")

  val nexus = "https://oss.sonatype.org/"
  val SonatypeSnapshots = Some("snapshots" at nexus + "content/repositories/snapshots")
  val SonatypeReleases = Some("releases" at nexus + "service/local/staging/deploy/maven2")

  val Repository: Option[Resolver] = {
    (altReleaseDeploymentRepository, altReleaseDeploymentLocation) match {
      case (Some(name), Some(location)) => Some(name at location)
      case _ => if (Version.endsWith("SNAPSHOT")) {
        SonatypeSnapshots
      } else {
        SonatypeReleases
      }
    }
  }

  println(s"Using $Repository for publishing")

  lazy val inlineCredentials = for (
    realm ← sys.props.get("publish.repository.credentials.realm");
    host ← sys.props.get("publish.repository.credentials.host");
    user ← sys.props.get("publish.repository.credentials.user");
    password ← sys.props.get("publish.repository.credentials.password")
  ) yield Credentials(realm, host, user, password)

  val Creds: Seq[Credentials] = {
    val credFile = sys.props.get("publish.repository.credentials.file")
      .map(path => Credentials(new File(path)))

    val defaultCredentialsLocation = Some(Credentials(Path.userHome / ".ivy2" / ".credentials"))

    val personalCred = Some(Credentials(Path.userHome / ".sbt" / "credentials"))
    val deployCred = Some(Credentials(Path.userHome / ".sbt" / "credentials.deploy"))

    println(s"Reading credentials from $credFile")

    Seq(credFile, defaultCredentialsLocation,  personalCred, deployCred).flatten
  }


  val License =
    <licenses>
      <license>
        <name>Apache License Version 2.0</name>
        <url>https://www.apache.org/licenses/LICENSE-2.0</url>
        <distribution>repo</distribution>
        <comments />
      </license>
    </licenses>

  val OurScmInfo =
    Some(
      ScmInfo(
        url("https://github.com/datastax/spark-cassandra-connector"),
        "scm:git@github.com:datastax/spark-cassandra-connector.git"
      )
    )

  val OurDevelopers =
      <developers>
        <developer>
          <id>pkolaczk</id>
          <name>Piotr Kolaczkowski</name>
          <url>http://github.com/pkolaczk</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>jacek-lewandowski</id>
          <name>Jacek Lewandowski</name>
          <url>http://github.com/jacek-lewandowski</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>helena</id>
          <name>Helena Edelson</name>
          <url>http://github.com/helena</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>alexliu68</id>
          <name>Alex Liu</name>
          <url>http://github.com/alexliu68</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>RussellSpitzer</id>
          <name>Russell Spitzer</name>
          <url>http://github.com/RussellSpitzer</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>artem-aliev</id>
          <name>Artem Aliev</name>
          <url>http://github.com/artem-aliev</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>bcantoni</id>
          <name>Brian Cantoni</name>
          <url>http://github.com/bcantoni</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>jtgrabowski</id>
          <name>Jaroslaw Grabowski</name>
          <url>http://github.com/jtgrabowski</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
      </developers>
      <contributors>
        <contributor>
          <name>Andrew Ash</name>
          <url>http://github.com/ash211</url>
        </contributor>
        <contributor>
          <name>Luis Angel Vicente Sanchez</name>
          <url>http://github.com/lvicentesanchez</url>
        </contributor>
        <contributor>
          <name>Todd</name>
          <url>http://github.com/tsindot</url>
        </contributor>
        <contributor>
          <name>Li Geng</name>
          <url>http://github.com/anguslee</url>
        </contributor>
        <contributor>
          <name>Isk</name>
          <url>http://github.com/criticaled</url>
        </contributor>
        <contributor>
          <name>Holden Karau</name>
          <url>http://github.com/holdenk</url>
        </contributor>
        <contributor>
          <name>Philipp Hoffmann</name>
          <url>http://github.com/philipphoffmann</url>
        </contributor>
      </contributors>
    

}