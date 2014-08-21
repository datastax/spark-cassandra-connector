package com.datastax.spark.connector.util

import org.apache.spark.Logging

import scala.util.parsing.combinator.RegexParsers

object CqlWhereParser extends RegexParsers with Logging {

  def identifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ { id => Identifier(id.toLowerCase)}

  def quotedIdentifier = "\"" ~> "(\"\"|[^\"])*".r <~ "\"" ^^ { id => Identifier(id.toString)}

  def num = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r

  def bool = "true" | "false"

  def str = """'(''|[^'])*'""".r

  def uuid = """[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""".r

  def param: Parser[Literal] = ("?" | uuid | num | bool | str) ^^ {
    case "?" => QParam()
    case param => Param(param.toString)
  }

  def op = "<=" | ">=" | "=" | ">" | "<"
  def inOp = "(?i)in".r ^^ { _=> "in"}
  def andOp = "(?i)and".r ^^ {_ => "and"}

  def inParam = "(" ~> param ~ rep("," ~ param) <~ ")" ^^ {
    case param ~ list => list.foldLeft(List[Literal](param)) {
      case (params, "," ~ param) => params :+ param
    }
  }

  def expr: Parser[Predicate] = (((identifier | quotedIdentifier) ~ (op | inOp) ~ ( param | inParam)) | ".*".r) ^^ {
    case (Identifier(name) ~ "in" ~ QParam()) => new InPredicate(name)
    case (Identifier(name) ~ "in" ~ inParam) => InPredicateList(name, inParam.asInstanceOf[List[Literal]])
    case (Identifier(name) ~ "=" ~ param) => EqPredicate(name, param.asInstanceOf[Literal])
    case (Identifier(name) ~ op ~ param) => RangePredicate(name, Operator(op.asInstanceOf[String]), param.asInstanceOf[Literal])
    case (unknown) => UnknownPredicate("", unknown.toString)
  }

  def where = expr ~ rep(andOp ~ expr) ^^ {
    case expr ~ list => list.foldLeft(List[Predicate](expr)) {
      case (exprs, "and" ~ expr2) => exprs :+ expr2
    }
  }

  def predicates(s: String): Seq[Predicate] = {
    parseAll(where, s) match {
      case Success(columns, _) => columns
      case x => logError("Where predicate parsing error:" + x.toString); List()
    }
  }

}

trait Literal
case class Operator(op: String) extends Literal
case class Identifier(name: String) extends Literal
case class Param(value: Any) extends Literal
case class QParam() extends Literal

trait Predicate {
  def columnName: String
}
case class InPredicate(columnName: String) extends Predicate
case class InPredicateList(columnName: String, values: List[Literal]) extends Predicate
case class EqPredicate(columnName: String, value: Literal) extends Predicate
case class RangePredicate(columnName: String, operator: Operator, value: Literal) extends Predicate
case class UnknownPredicate(columnName: String, text: String) extends Predicate

// for completeness only
