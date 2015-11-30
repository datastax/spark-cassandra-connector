import java.io.IOException
import java.net.URLDecoder
import java.nio.file.Paths

import org.xml.sax.SAXParseException
import sbt._

import scala.xml.XML

object BuildUtil {

  private def mavenLocalDir = {
    def loadHomeFromSettings(f: () => File): Option[String] =
      try {
        val file = f()
        if (!file.exists) None
        else (XML.loadFile(file) \ "localRepository").text match {
          case "" => None
          case e@_ => Some(e)
        }
      } catch {
        // Occurs inside File constructor when property or environment variable does not exist
        case _: NullPointerException => None
        // Occurs when File does not exist
        case _: IOException => None
        case e: SAXParseException =>
          System.err.println(s"Problem parsing ${f().getAbsolutePath}, ${e.getMessage}")
          None
      }
    sys.env.get("M2_REPO") orElse
      loadHomeFromSettings(() => new File(Path.userHome, ".m2/settings.xml")) orElse
      loadHomeFromSettings(() => new File(new File(System.getenv("M2_HOME")), "conf/settings.xml")) getOrElse
      Paths.get(Path.userHome.getAbsolutePath, ".m2", "repository").toString
  }

  def resolveEnvVars(s: String): String = {
    val pattern = """\$\{env\.([^\}]+)\}""".r
    pattern.replaceAllIn(URLDecoder.decode(s, "utf-8"),
      matched => sys.env.getOrElse(matched.group(1), ""))
  }

  val mavenLocalResolver = {
    val repoPath = mavenLocalDir
    "Maven local repository" at Paths.get(resolveEnvVars(repoPath)).toUri.toString
  }

  case class DocumentationMapping(url: URL, jarFileMatcher: Attributed[File] ⇒ Boolean)

  object DocumentationMapping {
    def apply(url: String, pattern: String): DocumentationMapping = {
      val regex = pattern.r
      new DocumentationMapping(new URL(url), file ⇒ regex.findPrefixOf(file.data.getName).isDefined)
    }

    def apply(url: URL, moduleIds: ModuleID*): DocumentationMapping = {
      def matches(fileName: String)(moduleID: ModuleID): Boolean = {
        fileName.matches(s"${moduleID.name}.*\\.jar")
      }

      val matcher = { file: Attributed[File] ⇒ moduleIds.exists(matches(file.data.getName)) }
      new DocumentationMapping(url, matcher)
    }

    def mapJarToDocURL(
      files: Seq[Attributed[File]],
      mappings: Seq[DocumentationMapping]
    ): Map[File, URL] = {

      val foundMappings =
        for (file ← files; mapping ← mappings if mapping.jarFileMatcher(file))
          yield file.data → mapping.url

      foundMappings.toMap
    }

  }

}
