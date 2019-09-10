import java.io.File

import sbt.Path
import sbt._
import sbt.librarymanagement.Resolver
import sbt.librarymanagement.ivy.Credentials

import scala.sys.process._

object Publishing extends sbt.librarymanagement.DependencyBuilders {

  val DseRelease = "datastax-releases-local" at "https://repo.sjc.dsinternal.org/artifactory/datastax-releases-local"

  val Version: String = {
    sys.props.get("publish.version").getOrElse("git describe --tags" !!).stripLineEnd
  }

  val Repository: Option[Resolver] = {
    Some((sys.props.get("publish.repository.name"), sys.props.get("publish.repository.location")) match {
      case (Some(name), Some(location)) => name at location
      case _ =>  Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository"))
    })
  }

  val Credentials: Seq[Credentials] = {
    val credFile = sys.props.get("publish.repository.credentials.file")
      .map(path => sbt.librarymanagement.ivy.Credentials(new File(path)))


    val personalCred = sbt.librarymanagement.ivy.Credentials(Path.userHome / ".sbt" / "credentials")
    val deployCred = sbt.librarymanagement.ivy.Credentials(Path.userHome / ".sbt" / "credentials.deploy")

    println(s"Reading credentials from $credFile")

    Seq(credFile, Some(personalCred), Some(deployCred)).flatten
  }


}