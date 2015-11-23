package com.datastax.spark.connector.util

import org.apache.commons.lang3.StringUtils

import com.datastax.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory}

object PropertiesFileBuilder {

  val Header =
    """# Configuration Reference
    """.stripMargin
  val Footer = "\n"

  val allConfigs = ConfigCheck.validStaticProperties

  def getPropertiesFileContent(): String = {
    val longestParamLength = allConfigs.map(_.name.length).max
    val configBySection = allConfigs.groupBy(x => x.section)
    val sections = configBySection.keys.toSeq.sorted
    val propertiesContent = for (section <- sections) yield {
      val parameters = configBySection.get(section).get
      val paramList = parameters.toList.sortBy(_.name).map { case parameter: ConfigParameter[_] =>
        val default = parameter.default match {
          case x: CassandraConnectionFactory => x.getClass.getCanonicalName.stripSuffix("$")
          case x: AuthConfFactory => x.getClass.getCanonicalName.stripSuffix("$")
          case x: Iterable[_] => x.mkString(",")
          case Some(defaultValue) => defaultValue
          case None => ""
          case x: Enum[_] => x.name()
          case value => value
        }
        val description = parameter.description
          .replaceAll("<br/?>", "\n")
          .replaceAll("</?code>", "")
          .replaceAll("<li>", " - ")
          .replaceAll("</li>", "")
          .replaceAll("</?[uo]l>", "")
          .replaceAll("\n\n", "\n")
          .trim
          .replaceAll("\n", "\n# ")
          .replaceAll("  ", " ")

        s"""# $description
            |# ${StringUtils.rightPad(parameter.name, longestParamLength, ' ')} $default
            |""".stripMargin
      }.mkString("\n")

      s"""
         |## $section
         |
         |$paramList""".stripMargin
    }
    Header + propertiesContent.mkString("\n") + Footer
  }

}
