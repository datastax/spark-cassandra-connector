package com.datastax.spark.connector.util

import com.datastax.driver.core.Metadata

import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

object NameTools {

  val MinJWScore = 0.85

  /* Example JaroWinklerScores
  {{{
  val wordpairs = Seq(
  ("banana","bananastand"),
  ("apple","bananastand"),
  ("customer_id","customerid"),
  ("custid","customerid"),
  ("liberty","freedom"),
  ("spark","spork"),
  ("test","testt"))
  wordpairs.foreach( p =>
    println(s"${p._1} ~ ${p._2} = ${StringUtils.getJaroWinklerDistance(p._1,p._2)}"))
  }}}
  banana ~ bananastand = 0.91
  apple ~ bananastand = 0.43
  customer_id ~ customerid = 0.98
  custid ~ customerid = 0.4
  liberty ~ freedom = 0.36
  spark ~ spork = 0.89
  test ~ testt = 0.96 */

  sealed trait Suggestions

  case class TableSuggestions(tables: Seq[(String)]) extends Suggestions
  case class KeyspaceSuggestions(keyspaces: Seq[String]) extends Suggestions
  case class KeyspaceAndTableSuggestions(keyspaceTablePairs: Seq[(String, String)]) extends Suggestions


  /**
   * Given a Cluster Metadata Object determine if there are any fuzzy matches to the keyspace and table
   * passed in. We attempt to only show relevant results by returning suggestions with the following priority:
   * 1. Perfect match on keyspace name but only fuzzy match on table name else
   * 2. Perfect match on table name but only a fuzzy match on the keyspace name else
   * 3. Fuzzy match on keyspace and fuzzy match on table name else
   * 4. Fuzzy match on table no match on keyspace else
   * 5. None
   */
  def getSuggestions(clusterMetadata: Metadata, keyspace: String, table: String): Option[Suggestions] = {

    val keyspaceScores = clusterMetadata
      .getKeyspaces
      .map(ks =>
      (ks, StringUtils.getJaroWinklerDistance(ks.getName.toLowerCase, keyspace.toLowerCase)))

    val ktScores = for ((ks, ksScore) <- keyspaceScores; t <- (ks.getTables ++ ks.getMaterializedViews)) yield {
      val tScore = StringUtils.getJaroWinklerDistance(t.getName.toLowerCase, table.toLowerCase)
      (ks.getName, t.getName, ksScore, tScore)
    }

    //1. Perfect match on keyspace, fuzzy match on table
    val suggestedTables = ktScores
      .collect { case (ks, t, ksScore, tScore) if ks == keyspace && tScore > MinJWScore => t}

    //2. Fuzzy match on table, perfect match on table
    val suggestedKeyspaces = ktScores
      .collect { case (ks, t, ksScore, tScore) if ksScore > MinJWScore && t == table => ks}

    //3. Fuzzy match on keyspace, fuzzy match on table
    val suggestedKeyspaceAndTables = ktScores
      .collect { case (ks, t, ksScore, tScore) if ksScore > MinJWScore && tScore > MinJWScore => (ks, t)}

    //4. No match on keyspace, fuzzy match on table
    val suggestedTablesUnknownKeyspace = ktScores
      .collect { case (ks, t, ksScore, tScore) if tScore > MinJWScore => (ks, t)}

    if (suggestedTables.nonEmpty) Some(TableSuggestions(suggestedTables))
    else if (suggestedKeyspaces.nonEmpty) Some(KeyspaceSuggestions(suggestedKeyspaces))
    else if (suggestedKeyspaceAndTables.nonEmpty) Some(KeyspaceAndTableSuggestions(suggestedKeyspaceAndTables))
    else if (suggestedTablesUnknownKeyspace.nonEmpty) Some(KeyspaceAndTableSuggestions(suggestedTablesUnknownKeyspace))
    else None
  }

  def getErrorString(keyspace: String, table: String, suggestion: Option[Suggestions]): String = suggestion match {
    case None => s"Couldn't find $keyspace.$table or any similarly named keyspace and table pairs"
    case Some(TableSuggestions(tables)) => s"Couldn't find table $table in $keyspace - Found similar tables in that keyspace:\n${tables.map(t => s"$keyspace.$t").mkString("\n")}"
    case Some(KeyspaceSuggestions(keyspaces)) => s"Couldn't find table $table in $keyspace - Found similar keyspaces with that table:\n${keyspaces.map(k => s"$k.$table").mkString("\n")}"
    case Some(KeyspaceAndTableSuggestions(kt)) => s"Couldn't find table $table or keyspace $keyspace - Found similar keyspaces and tables:\n${kt.map { case (k, t) => s"$k.$t"}.mkString("\n")}"
  }
}
