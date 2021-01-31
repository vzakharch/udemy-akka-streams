package part2primer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}

import scala.util.{Failure, Success}

object MaterializingStreams extends App {
  implicit val system: ActorSystem = ActorSystem("MaterializingStreams")
  import system.dispatcher

  val simpleGraph: RunnableGraph[NotUsed] = Source(1 to 10).to(Sink.foreach(println))

//  val result = simpleGraph.run()

  val source = Source(1 to 10)
  val sink = Sink.reduce[Int]{ _ + _ }
  //  val sumFuture = source.runWith(sink)
  //  sumFuture.onComplete{
  //    case Success(value) => println(s"Sum of elements is: $value")
  //    case Failure(exception) => println(s"Failed with error: $exception")
  //  }

  //choosing values
  val simpleSource = Source(1 to 10)
  val simpleFlow = Flow[Int].map{ _ + 1}
  val simpleSink = Sink.foreach[Int](println)

  val graph = simpleSource
    .viaMat(simpleFlow)(Keep.right)
    .toMat(simpleSink)(Keep.right)

//  graph.run().onComplete{
//    case Success(_) => println("Dunzo")
//    case Failure(ex) => println(s"Error: $ex")
//  }

//  Source(1 to 10).runWith(Sink.reduce[Int](_ + _))
//  Source(1 to 10).runReduce(_ + _)

//  Sink.foreach[Int](println).runWith(Source.single(33))

//  Flow[Int].map{2 * _}.runWith(simpleSource, simpleSink)

  /*
   * return the last element out of the source
   * Sink.last
   *
   * total wc out of stream of sentences
   *    map fold reduce
   */

  val lastSource = Source[Int](1 to 5)

  val aaa = lastSource.toMat(Sink.last[Int])(Keep.right).run()
    .onComplete {
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }
  val bbb = lastSource.runWith(Sink.last)
    .onComplete {
      case Success(value) => println(value)
      case Failure(exception) => println(exception)
    }
  ///////////////////////////////////////////////////////////////////////////
  val text = List(
    "Sencence one",
    "Sencence two asf",
    "Sencence three gfd",
    "Sencence four",
    "Sencence seven",
  )

  val textSource = Source(text)
  val countFlow = Flow.fromFunction[String,Int](line => line.split(" ").length)
  val sinkCount = Sink.reduce[Int]{ _ + _}

  textSource.viaMat(countFlow)(Keep.right).runWith(sinkCount).onComplete{
    case Success(value) => println(s"Count: $value")
    case Failure(exception) => println(exception)

  }
}
