package com.datastax.spark.connector.util

import org.apache.spark.Logging
import scala.util.parsing.combinator.RegexParsers

object CqlWhereParser extends RegexParsers with Logging {
  override type Elem = Char
  def identifier : Parser[String]   = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ { _.toLowerCase}
  def quotedIdentifier: Parser[String]   = "\"" ~> "(\"\"|[^\"])*".r <~  "\"" ^^{ _.toString }
  def num = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r
  def bool = "true" | "false"
  def str = """'(''|[^'])*'""".r
  def uuid = """[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""".r
  def param = "?" |uuid | num | bool | str  ^^ { _.toString }
  def op = "<=" |">=" | "=" | ">" | "<"
  def inParam  =  "(" ~> param ~ rep("," ~ param) <~ ")" ^^
    {
      case  param ~ list  => list.foldLeft(List[String](param)) {
        case (params,  "," ~ param ) => params :+ param.toString
      }
    }
  def expr = (identifier | quotedIdentifier) ~ (op | "in") ~  (param | inParam)  ^^
    {
      case (id ~ "in" ~ "?") => new ParsedColumn(id, List[String]())
      case (id ~ "in" ~ inParam) => new ParsedColumn(id, inParam.asInstanceOf[List[String]])
      case (id ~ op ~ param) => new ParsedColumn(id, List(param.asInstanceOf[String]))
    }
  def where:Parser[List[ParsedColumn]] =  expr ~ rep( "and" ~ expr) ^^
    {
      case expr ~ list => list.foldLeft(List[ParsedColumn](expr)) {
        case (exprs,  "and" ~ expr2 ) => exprs :+ expr2
      }
    }

  def columns (s: String): Seq[ParsedColumn] = {
    parseAll(where, s) match {
      case Success(columns, _) => columns
      case x => logError("Where predicate parsing error:" + x.toString);List()
    }
  }
  class ParsedColumn (n: String,vs: List[String]) {
    var name = n
    var values = vs
  }
}

