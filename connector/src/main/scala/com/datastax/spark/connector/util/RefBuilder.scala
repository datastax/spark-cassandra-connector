package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory}

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
    val configBySection = allConfigs.groupBy(x => x.section)
    val sections = configBySection.keys.toSeq.sorted
    val markdown = for (section <- sections) yield {
      val parameters = configBySection(section)
      val paramTable = parameters.toList.sortBy(_.name).map { case parameter: ConfigParameter[_] =>
        val default = parameter.default match {
          case x: CassandraConnectionFactory => x.getClass.getSimpleName.stripSuffix("$")
          case x: AuthConfFactory => x.getClass.getSimpleName.stripSuffix("$")
          case x: Seq[_] => x.mkString(",")
          case Some(defaultValue) => defaultValue
          case None => None
          case value => value
        }
        s"""<tr>
            |  <td><code>${parameter.name}</code></td>
            |  <td>$default</td>
            |  <td>${parameter.description}</td>
            |</tr>""".stripMargin
      }.mkString("\n")

      s"""
         |## $section
         |$HtmlTableHeader
         |$paramTable
         |</table>""".stripMargin
    }
    Header + markdown.mkString("\n\n") + Footer
  }

}
