package part2primer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object FirstPrinciples extends App {
  implicit val system: ActorSystem = ActorSystem("FirstPrinciples")

  val source: Source[Int, NotUsed] = Source(1 to 10)
  // sink
  val sink = Sink.foreach[Int](println)

  val graph = source to sink
  //graph.run()

  val flow = Flow[Int].map(_ + 1)
  val sourceWithFlow = source via flow
  val flowWithSink = flow to sink

  //  sourceWithFlow to sink run()
  //  source to flowWithSink run()
  //  source via flow to sink run()

  // nulls are not allowed
  //val illegalSource = Source.single[String](null)
  //  illegalSource.to(Sink.foreach(_ => println())).run()


  val finiteSource = Source.single(1)
  val anotherOne = Source(List(1,2,3))
  val empty = Source.empty[Int]
  val infiniteSource = Source(Stream.from(1))
  import scala.concurrent.ExecutionContext.Implicits.global
  val futureSource = Source.future(Future(42))

  //sinks
  val ignoring = Sink.ignore
  val foreachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int]
  val foldSink = Sink.fold[Int, Int](0) { _ + _ }

  //flows (mapped to collection operations)
  val doubleFlow = Flow[Int].map {2 * _}
  val takeFlow = Flow[Int].take(5)

  //  val  doubleFlowGraph = source via doubleFlow via takeFlow to sink
  //  doubleFlowGraph.run()

  val mapSource = Source(1 to 10).map {_ * 2}

  /*
        Create stream names of persons
        Keep first 2 names with length > 5 chars
   */
  val names = List("Dave", "Alison", "Jordan", "Bob", "Ron", "George", "Victor", "Sam", "Paul")

  val nameSource = Source[String](names)
  val longNames = Flow[String].filter{_.length > 5}
  val limit = Flow[String].take(2)
  val printSink = Sink.foreach[String](println)

  val result = nameSource
    .via(longNames)
    .via(limit)
    .to(printSink)
    .run()


    implicit val ec = system.dispatcher
    source.runForeach( _ => ()).onComplete(_ => system.terminate())
}
