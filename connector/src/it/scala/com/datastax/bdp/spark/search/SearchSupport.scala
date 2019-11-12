package com.datastax.bdp.spark.search

import java.net.InetAddress

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.solr.client.solrj.impl.HttpSolrClient

import scala.collection.concurrent.TrieMap

trait SearchSupport {

  def getSolrClient(host: InetAddress, indexName: String): HttpSolrClient = {
    SearchSupport.solrClients.getOrElseUpdate(indexName, {
      new HttpSolrClient.Builder("http://" + host.getHostName + ":" + SearchSupport.solrPort + "/solr/" + indexName).build()
    })
  }

  def createCore(session: CqlSession, host: Int, indexName: String): Unit = {
    createCore(session, host, indexName, true, false, true, false)
  }

  def createCore(session: CqlSession, host: Int, indexName: String, distributed: Boolean, recovery: Boolean, reindex: Boolean, lenient: Boolean = false): Unit = {
    val split = indexName.split("\\.", 2)
    val (ks, table) = (split(0), split(1))

    val createStatement =
      s"""CREATE SEARCH INDEX IF NOT EXISTS ON "$ks"."$table"
         |WITH OPTIONS{distributed:$distributed,recovery:$recovery,reindex:$reindex,lenient:$lenient}""".stripMargin

    session.execute(createStatement)
  }
}

object SearchSupport {
  private val solrPort = 8983
  private val solrClients = new TrieMap[String, HttpSolrClient]
}