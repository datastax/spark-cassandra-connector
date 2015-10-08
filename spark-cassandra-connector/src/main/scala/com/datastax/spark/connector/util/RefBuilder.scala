package com.datastax.spark.connector.util

case class ConfigParameter (
  val name:String,
  val section: String,
  val default:Option[Any],
  val description:String )

object RefBuilder {

  val Header =
    """# Configuration Reference
      |
      |
    """.stripMargin
  val Footer = "\n"

  val HtmlTableHeader =
    """<table class="table">
      |<tr><th>Property Name</th><th>Default</th><th>Description</th></tr>""".stripMargin

  val allConfigs = ConfigCheck.validStaticProperties
  
  def getMarkDown(): String = {
    val configBySection = allConfigs.groupBy( x => x.section)
    val sections = configBySection.keys.toSeq.sorted
    val markdown = for (section <- sections) yield {
      val parameters = configBySection.get(section).get
      val paramTable = parameters.toList.sortBy( _.name).map { case parameter: ConfigParameter =>
        val default = parameter.default match {
          case Some(defaultValue) => defaultValue
          case None => None
        }
        s"""<tr>
           |  <td><code>${parameter.name.stripPrefix("spark.cassandra.")}</code></td>
           |  <td>$default</td>
           |  <td>${parameter.description}</td>
           |</tr>""".stripMargin
      }.mkString("\n")

      s"""
           |## $section
           |**All parameters should be prefixed with <code> spark.cassandra. </code>**
           |
           |$HtmlTableHeader
           |$paramTable
           |</table>""".stripMargin
    }
    Header + markdown.mkString("\n\n") + Footer
  }

}
