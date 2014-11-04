package com.datastax.spark.connector.embedded

import akka.actor.ActorRef

object Event {

  sealed trait Status extends Serializable

  case class ReceiverStarted(ref: ActorRef) extends Status

  case class Pushed(data: AnyRef) extends Status

  case object Completed extends Status

  case object Report extends Status

  case class WordCount(word: String, count: Int) extends Serializable

}
