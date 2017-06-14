package com.datastax.spark.connector.util

import scala.collection.immutable.{TreeMap, TreeSet}
import scala.reflect.runtime.universe._

import org.apache.commons.lang3.tuple

import com.datastax.spark.connector.types.CassandraOption

object Symbols {
  val CassandraOptionSymbol = typeOf[CassandraOption[Any]].asInstanceOf[TypeRef].sym
  val OptionSymbol = typeOf[Option[Any]].asInstanceOf[TypeRef].sym
  val ListSymbol = typeOf[List[Any]].asInstanceOf[TypeRef].sym
  val VectorSymbol = typeOf[Vector[Any]].asInstanceOf[TypeRef].sym
  val SetSymbol = typeOf[Set[Any]].asInstanceOf[TypeRef].sym
  val TreeSetSymbol = typeOf[TreeSet[Any]].asInstanceOf[TypeRef].sym
  val SeqSymbol = typeOf[Seq[Any]].asInstanceOf[TypeRef].sym
  val IndexedSeqSymbol = typeOf[IndexedSeq[Any]].asInstanceOf[TypeRef].sym
  val IterableSymbol = typeOf[Iterable[Any]].asInstanceOf[TypeRef].sym
  val MapSymbol = typeOf[Map[Any, Any]].asInstanceOf[TypeRef].sym
  val TreeMapSymbol = typeOf[TreeMap[Any, Any]].asInstanceOf[TypeRef].sym

  val JavaListSymbol = typeOf[java.util.List[Any]].asInstanceOf[TypeRef].sym
  val JavaArrayListSymbol = typeOf[java.util.ArrayList[Any]].asInstanceOf[TypeRef].sym
  val JavaSetSymbol = typeOf[java.util.Set[Any]].asInstanceOf[TypeRef].sym
  val JavaHashSetSymbol = typeOf[java.util.HashSet[Any]].asInstanceOf[TypeRef].sym
  val JavaMapSymbol = typeOf[java.util.Map[Any, Any]].asInstanceOf[TypeRef].sym
  val JavaHashMapSymbol = typeOf[java.util.HashMap[Any, Any]].asInstanceOf[TypeRef].sym

  val PairSymbol = typeOf[tuple.Pair[Any, Any]].asInstanceOf[TypeRef].sym
  val TripleSymbol = typeOf[tuple.Triple[Any, Any, Any]].asInstanceOf[TypeRef].sym

  val ListSymbols = Set(
    ListSymbol, VectorSymbol, SeqSymbol, IndexedSeqSymbol, IterableSymbol,
    JavaListSymbol, JavaArrayListSymbol)
  val SetSymbols = Set(SetSymbol, TreeSetSymbol, JavaSetSymbol, JavaHashSetSymbol)
  val MapSymbols = Set(MapSymbol, TreeMapSymbol, JavaMapSymbol, JavaHashMapSymbol)
}
