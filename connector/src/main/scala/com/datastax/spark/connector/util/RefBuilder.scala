package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory}

object RefBuilder {

  val AsciidocHeader =
    """= Configuration reference
      |
      |
    """.stripMargin

  val AsciidocFooter = "\n"

  val AsciidocTableHeader =
    """[cols=",,",options="header",]
    #|===
    #|Property Name |Default |Description""".stripMargin('#')


  val allConfigs = ConfigCheck.validStaticProperties

  def getAsciidoc(): String = {
    val configBySection = allConfigs.groupBy(x => x.section)
    val sections = configBySection.keys.toSeq.sorted
    val asciidoc = for (section <- sections) yield {
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
        s"""
            #|`+${parameter.name}+` | $default | ${parameter.description} """.stripMargin('#')
      }.mkString("\n")

      s"""
         #== $section
         #
         #$AsciidocTableHeader
         #$paramTable
         #|===""".stripMargin('#')
    }
    AsciidocHeader + asciidoc.mkString("\n\n") + AsciidocFooter
  }

}
