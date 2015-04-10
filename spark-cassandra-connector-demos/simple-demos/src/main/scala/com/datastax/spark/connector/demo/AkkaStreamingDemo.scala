package com.datastax.spark.connector.demo

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming.TypedStreamingActor
import com.datastax.spark.connector.embedded.Event._
import com.datastax.spark.connector.embedded.Assertions

/**
 * This demo can run against a single node, local or remote.
 * See the README for running the demos.
 * 1. Start Cassandra
 * 2. Start Spark:
 * 3. Run the demo from SBT with: sbt spark-cassandra-connector-demos/run
 *      Then enter the number for: com.datastax.spark.connector.demo.streaming.AkkaStreamingDemo
 *    Or right click to run in an IDE
 *
 * Note: For our initial streaming release we started with Akka integration of Spark Streaming.
 * However coming soon is Kafka, ZeroMQ, then Twitter Streaming.
 */
object AkkaStreamingDemo extends App {

  /* Initialize Akka, Cassandra and Spark settings. */
  val settings = new SparkCassandraSettings()
  import settings._

  /** Configures Spark. */
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraSeed)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)
    .setMaster(SparkMaster)
    .setAppName("Streaming Akka App")

  /** Creates the keyspace and table in Cassandra. */
  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeyspace.$CassandraTable (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute(s"TRUNCATE $CassandraKeyspace.$CassandraTable")
  }

  /** Connect to the Spark cluster: */
  lazy val sc = new SparkContext(conf)

  /** Creates the Spark Streaming context. */
  val ssc = new StreamingContext(sc, Milliseconds(500))

  /** Creates the demo's Akka ActorSystem to easily insure dispatchers are separate and no naming conflicts.
    * Unfortunately Spark does not allow users to pass in existing ActorSystems. */
  val system = ActorSystem("AkkaStreamingDemo")

  /** Creates the root supervisor of this simple Akka [[akka.actor.ActorSystem$ ActorSystem]] node that
    * you might deploy across a cluster. */
  val guardian = system.actorOf(Props(new NodeGuardian(ssc, settings, Data)), "node-guardian")

}

/**
 * The NodeGuardian actor is the root supervisor of this simple Akka application's ActorSystem node that
 * you might deploy across a cluster.
 *
 * Being an Akka supervisor actor, it would normally orchestrate its children and any fault tolerance policies.
 * For a simple demo no policies are employed save that embedded, in the Akka actor API.
 *
 * Demo data for a simple but classic WordCount:
 * {{{
 *   val data = immutable.Set("words ", "may ", "count ")
 * }}}
 *
 * The NodeGuardian spins up three child actors (not in this order):
 *
 * 1. Streamer
 * A simple Akka actor which extends `com.datastax.spark.connector.streaming.TypedStreamingActor` and ultimately
 * implements a Spark `Receiver`. This simple receiver calls
 * {{{
 *    Receiver.pushBlock[T: ClassTag](data: T)
 * }}}
 * when messages of type `String` (for simplicity of a demo), are received. This would typically be data in some
 * custom envelope of a Scala case class that is Serializable.
 *
 * 2. Sender
 * A simple Akka actor which generates a pre-set number of random tuples based on initial input  `data` noted above,
 * and sends each random tuple to the [[Streamer]]. The random messages are generated and sent to the stream every
 * millisecond, with an initial wait of 2 milliseconds.
 *
 * 3. Verification
 * Calls the following on the `StreamingContext` (ssc) to know when the
 * expected number of entries has been streamed to Spark, and `scale` (the number of messages sent to the stream),
 * computed, and saved to Cassandra:
 * {{{
 *    val rdd = ssc.cassandraTable[WordCount](keyspaceName, tableName)
 *    rdd.collect.nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2
 * }}}
 *
 * Where `data` represents the 3 words we computed, we assert the expected three columns were created:
 * {{{
 *    assert(rdd.collect.length == data.size)
 * }}}
 *
 *@param ssc the Spark `StreamingContext`
 *
 * @param settings the [[SparkCassandraSettings]] from config
 *
 * @param data the demo data for a simple WordCount
 */
class NodeGuardian(ssc: StreamingContext, settings: SparkCassandraSettings, data: immutable.Set[String])
  extends CounterActor with ActorLogging with Assertions {

  import akka.util.Timeout
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
  import com.datastax.spark.connector.streaming._
  import settings._
  import context.dispatcher

  implicit val timeout = Timeout(5.seconds)

  /** Captures Spark's Akka ActorSystem. */
  private val sas = SparkEnv.get.actorSystem

  sas.eventStream.subscribe(self, classOf[ReceiverStarted])

  /** Creates an Akka Actor input stream, defines the computation in the stream,
    * and the action to write to Cassandra. */
  ssc.actorStream[String](Props[Streamer], "actor-stream", StorageLevel.MEMORY_AND_DISK)
    .flatMap(_.split("\\s+"))
    .map((_, 1))
    .reduceByKey(_ + _)
    .saveToCassandra(CassandraKeyspace, CassandraTable)


  /* Defines the work to do in the stream. Placing the import here to explicitly show
     that this is where the implicits are used for the DStream's 'saveToCassandra' functions: */
  import com.datastax.spark.connector.streaming._

  /** Once the stream and sender actors are created, the spark stream's compute configured, the `StreamingContext` is started. */
  ssc.start()
//  ssc.awaitTermination()
  log.info(s"Streaming context started.")

  def receive: Actor.Receive = {
    /** Initializes direct point-to-point messaging of event-driven data from [[Sender]] to [[Streamer]].
      * For purposes of a demo, we put an Akka DeathWatch on the stream actor, because this actor stops itself once its
      * work is `done` (again, just for a simple demo that does work and stops once expectations are met).
      * Then we inject the [[Sender]] actor with the [[Streamer]] actor ref so it can easily send data to the stream. */
    case ReceiverStarted(receiver) =>
      log.info(s"Spark Streaming actor located: $receiver")
      context.watch(receiver)
      context.actorOf(Props(new Sender(data.toArray, receiver)))

    /** Akka DeathWatch notification that `ref`, the [[Streamer]] actor we are watching, has terminated itself. */
    case Terminated(ref) => verify()
  }

  def done: Actor.Receive = {
    case Completed => shutdown()
  }

  def verify(): Unit = {
    val rdd = ssc.cassandraTable[WordCount](CassandraKeyspace, CassandraTable)

    def successful: Boolean = rdd.collect.nonEmpty && rdd.map(_.count).reduce(_ + _) == scale * 2

    awaitCond(successful, 3.minutes)
    rdd.collect foreach (row => log.info(s"New Data: $row"))

    context.become(done)
    self ! Completed
  }

  /** Stops the ActorSystem, the Spark `StreamingContext` and its underlying Spark system. */
  def shutdown(): Unit = {
    log.info(s"Assertions successful, shutting down.")
    context.system.eventStream.unsubscribe(self)
    log.info(s"Stopping the demo app actor system and '$ssc'")
    context.system.shutdown()
    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }

}

/** Simply showing what the streaming actor does for the sake of the demo. It is a
  * `org.apache.spark.streaming.receivers.Receiver`. This receiver tracks the number
  * of blocks of data pushed to Spark so that the demo can shut down once we assert
  * the expected data has been saved to Cassandra.
  *
  * The additional behavior of a Counter simply supports the demo shutdown once
  * the Stream has sent all the randomly generated data to the Spark `DStream` for
  * processing. Once completed, the [[Streamer]] triggers an Akka DeathWatch by
  * {{{ self ! PoisonPill }}}
  *
  * {{{trait CounterActor extends Actor  with Logging {
  *      protected val scale = 30
  *      private var count = 0
  *
  *      protected def increment(): Unit = {
  *         count += 1
  *         if (count == scale) self ! PoisonPill
  *       }
  *   } }}}
  */
class Streamer extends TypedStreamingActor[String] with CounterActor {

  override def preStart(): Unit =
    context.system.eventStream.publish(ReceiverStarted(self))

  override def push(e: String): Unit = {
    super.push(e)
    increment()
  }
}

/** A simple Akka actor which generates a pre-set number of random tuples based on initial input
  * `data`, and sends each random tuple to the [[Streamer]]. The random messages are generated
  * and sent to the stream every millisecond, with an initial wait of 2 milliseconds. */
class Sender(val data: Array[String], val to: ActorRef) extends Actor {
  import context.dispatcher

  private val rand = new scala.util.Random()

  val task = context.system.scheduler.schedule(2.second, 500.millis) {
    to ! createMessage()
  }

  override def postStop(): Unit = task.cancel()

  def createMessage(): String = {
    val x = rand.nextInt(3)
    data(x) + data(2 - x)
  }

  def receive: Actor.Receive = {
    case _ =>
  }
}

trait CounterActor extends Actor {

  protected val scale = 30

  private var count = 0

  protected def increment(): Unit = {
    count += 1
    if (count == scale) self ! PoisonPill
  }
}