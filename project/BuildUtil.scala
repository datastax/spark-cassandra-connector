import sbt._

object BuildUtil {

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
