package com.datastax.spark.connector.util

import java.util.UUID

import org.apache.spark.Logging

import scala.util.parsing.combinator.RegexParsers

class  CqlWhereParser (cql: String, qValues: Iterator[Any]) extends RegexParsers with Logging {

  def qValueIterator = qValues
  def cqlWhere = cql

  private def unquotedIdentifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ {
    id => Identifier(id.toLowerCase)
  }

  private def quotedIdentifier = "\"" ~> "(\"\"|[^\"])*".r <~ "\"" ^^ {
    def unEscapeQuotes(s: String) = s.replace("\"\"", "\"")
    id => Identifier(unEscapeQuotes(id.toString))
  }

  private def identifier = unquotedIdentifier | quotedIdentifier

  private def num = """-?\d+(\.\d*)?([eE][-\+]?\d+)?""".r ^^ NumberLiteral.apply

  private def bool = ("true" | "false") ^^ {
    s => BooleanLiteral(s.toBoolean)
  }

  private def str = "'" ~> """(''|[^'])*""".r <~ "'" ^^ {
    def unEscapeQuotes(s: String) = s.replace("''", "'")
    s => StringLiteral(unEscapeQuotes(s))
  }

  private def uuid = """[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""".r ^^ {
    s => UUIDLiteral(UUID.fromString(s))
  }

  private def placeholder = "?" ^^ (_ => Placeholder(qValueIterator.next))

  private def value: Parser[Value] = placeholder | uuid | num | bool | str

  private def relationalOperator: Parser[RelationalOperator] =
    "<="       ^^ (_ => LowerEqual) |
    ">="       ^^ (_ => GreaterEqual) |
    "<"        ^^ (_ => LowerThan) |
    ">"        ^^ (_ => GreaterThan) |
    "="        ^^ (_ => EqualTo) |
    "(?i)in".r ^^ (_ => In)

  private def valueList: Parser[ValueList] = "(" ~> value ~ rep("," ~> value) <~ ")" ^^ {
    case literal ~ list => ValueList(literal :: list :_*)
  }

  private def and = "(?i)and".r ^^ {_ => "and" }

  private def predicate: Parser[Predicate] = ((identifier ~ relationalOperator ~ (value | valueList)) | ".*".r) ^^ {
    case Identifier(name) ~ In ~ Placeholder(value)                          => InPredicate(name, value)
    case Identifier(name) ~ In ~ (list : ValueList)                   => InListPredicate(name, list)
    case Identifier(name) ~ EqualTo ~ (v: Value)                      => EqPredicate(name, v)
    case Identifier(name) ~ (op: RelationalOperator) ~ (param: Value) => RangePredicate(name, op, param)
    case other                                                        => UnknownPredicate(other.toString, qValueIterator.foldLeft(Seq[Any]()) {_ :+ _})
  }

  private def whereClause = predicate ~ rep(and ~> predicate) ^^ {
    case expr ~ list => expr :: list
  }

  def parse(): Seq[Predicate] = {
    parseAll(whereClause, cqlWhere) match {
      case Success(columns, _) => columns
      case x => logError("Parse error when parsing CQL WHERE clause:" + x.toString); List()
    }
  }
}

object CqlWhereParser {
  def parse(cql: String): Seq[Predicate] = parse(cql, Seq().iterator)
  def parse(cql: String, qValues: Seq[Any]): Seq[Predicate] = parse(cql, qValues.iterator)
  def parse(cql: String, qValues: Iterator[Any]): Seq[Predicate] =
      new CqlWhereParser(cql, qValues).parse()
  }

sealed trait RelationalOperator {
  def toCqlString(): String
}

case object EqualTo extends RelationalOperator  {
  def toCqlString() = "="
}
case object LowerThan extends RelationalOperator{
  def toCqlString() = "<"
}
case object LowerEqual extends RelationalOperator{
  def toCqlString() = "<="
}
case object GreaterThan extends RelationalOperator{
  def toCqlString() = ">"
}
case object GreaterEqual extends RelationalOperator{
  def toCqlString() = ">="
}
case object In extends RelationalOperator{
  def toCqlString() = "in"
}

trait Value{
  def toCqlString ():String
}

case class Placeholder(value: Any) extends Value {
  def toCqlString () = "?"
}

trait Literal extends Value { def value: Any }
case class StringLiteral(value: String) extends Literal {
  protected def quote(value: String) = "'" + value.replace("'", "''") + "'"
  def toCqlString () = quote(value)
}
case class NumberLiteral(value: String) extends Literal{
  def toCqlString () = value
}
case class BooleanLiteral(value: Boolean) extends Literal {
  def toCqlString () = if(value) "true" else "false"
}
case class UUIDLiteral(value: UUID) extends Literal {
  def toCqlString () = value.toString
}

case class ValueList(values: Value*) {
  def toCqlString () = "(" + values.map(_.toCqlString).mkString(", ") + ")"
}

case class Identifier(name: String) {
  protected def quote(name: String) = "\"" + name.replace("\"", "\"\"") + "\""
  def toCqlString () = quote(name)
}

trait Predicate {
  def toCqlString ():String
}

trait SingleColumnPredicate extends Predicate {
  def columnName: String
  protected def quoteName(name: String) = "\"" + name.replace("\"", "\"\"") + "\""
}
case class InPredicate(columnName: String, value: Any) extends SingleColumnPredicate {
  def toCqlString () = quoteName(columnName) + " in ?"
}
case class InListPredicate(columnName: String, values: ValueList) extends SingleColumnPredicate {
  def toCqlString () = quoteName(columnName) + " in " + values.toCqlString()
}
case class EqPredicate(columnName: String, value: Value) extends SingleColumnPredicate {
  def toCqlString () = quoteName(columnName) + " = " + value.toCqlString()
}
case class RangePredicate(columnName: String, operator: RelationalOperator, value: Value) extends SingleColumnPredicate {
  def toCqlString () = quoteName(columnName) + " " + operator.toCqlString() + " " + value.toCqlString()
}
case class UnknownPredicate(text: String, qValues: Seq[Any]) extends Predicate {
  def toCqlString () = text
}

