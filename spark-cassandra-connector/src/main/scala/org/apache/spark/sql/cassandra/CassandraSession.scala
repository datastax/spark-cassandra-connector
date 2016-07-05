package org.apache.spark.sql.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.SessionState

class CassandraSession(sc: SparkContext) extends SparkSession(sc) {

  import CassandraSession._

  override lazy val sessionState: SessionState = new CassandraSessionState(this)

}

object CassandraSession {

  class CassandraSessionState(session: CassandraSession) extends SessionState(session) {
    override lazy val catalog: SessionCatalog = new CassandraCatalog(
      session,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }

}

