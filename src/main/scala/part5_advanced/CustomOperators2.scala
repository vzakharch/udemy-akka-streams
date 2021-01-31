package part5_advanced

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success}

object CustomOperators2 extends App {
  implicit val system = ActorSystem("CustomOperators")

  // 1 custom source  - emits random numbers until cancelled

  class RandomNumberGenerator(max: Int) extends GraphStage[SourceShape[Int]] {

    val outPort = Outlet[Int]("randomGenerator")
    val random = new Random()

    override def shape: SourceShape[Int] = SourceShape(outPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // logic
      setHandler(outPort, () => {
        // emit element
        push(outPort, random.nextInt(max))
      })
    }
  }

  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))

  //  randomGeneratorSource.runWith(Sink.foreach(println))

  class Batcher[T](batchSize: Int) extends GraphStage[SinkShape[T]] {

    val inPort: Inlet[T] = Inlet[T]("batcher")

    override def shape: SinkShape[T] = SinkShape[T](inPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      override def preStart(): Unit = {
        pull(inPort)
      }

      // Mutable state
      val batch = new mutable.Queue[T]

      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
          val nextElem = grab(inPort)
          batch.enqueue(nextElem)
          if (batch.size >= batchSize) {
            Thread.sleep(1000)
            println("New batch: " + batch.dequeueAll(_ => true).mkString("[", ", ", "]"))
          }
          pull(inPort) // Send demand
        }

        override def onUpstreamFinish(): Unit = {
          if (batch.nonEmpty) {
            println("New batch: " + batch.dequeueAll(_ => true).mkString("[", ", ", "]"))
            println("Stream finished.")
          }
        }
      })
    }
  }

  val batcherSink = Sink.fromGraph(new Batcher[Int](10))

  //  randomGeneratorSource.to(batcherSink).run()

  /**
    * Exercise: a custom flow - a simple filter flow
    * - 2 ports: an input port and an output port
    */

  class FilterFlow[T](predicate: T => Boolean) extends GraphStage[FlowShape[T, T]] {

    val inPort: Inlet[T] = Inlet[T]("filterInlet")
    val outPort: Outlet[T] = Outlet[T]("filterOutlet")

    override def shape: FlowShape[T, T] = FlowShape.of(inPort, outPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(inPort, () => {
        val nextElem = grab(inPort)
        if (predicate(nextElem)) {
          push(outPort, nextElem)
        } else {
          pull(inPort)
        }
      })
      setHandler(outPort, () => {
        pull(inPort)
      })
    }
  }

//  val oddOnes = Flow.fromGraph(new FilterFlow[Int](_ > 90))
//  randomGeneratorSource
//    .via(oddOnes)
//    .to(batcherSink).run()
//
  /*
      Materialized values in graph stages
   */

  class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T,T], Future[Int]] {
    val inPort = Inlet[T]("counterInt")
    val outPort = Outlet[T]("counterOut")

    override val shape = FlowShape(inPort, outPort)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]
      val logic = new GraphStageLogic(shape) {
        // setting mutable state
        var counter = 0
        setHandler(outPort, new OutHandler {
          override def onPull(): Unit = pull(inPort)

          override def onDownstreamFinish(cause: Throwable): Unit = {
            println(">>"+cause)
            promise.failure(cause)
            super.onDownstreamFinish(cause)
          }
        })
        setHandler(inPort, new InHandler {
          override def onPush(): Unit = {
            val nextelement = grab(inPort)
            counter+=1
            push(outPort, nextelement)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }
        })
      }
      (logic, promise.future)
    }
  }

  val counterFlow = Flow.fromGraph(new CounterFlow[Int])


  val result = Source(1 to 10)
    .viaMat(counterFlow)(Keep.right)
    .map(elem => if(elem==4) throw new RuntimeException("Error") else elem)
    .to(Sink.foreach(println))
    .run()

  result.onComplete{
    case Success(value) => println(s"Result: $value")
    case Failure(exception) => println(s"Failed with: ${exception.getMessage}")
  }(system.dispatcher)
}