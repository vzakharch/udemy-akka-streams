package akka.doc.quickstart

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream._
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class GraphDSLDocSpec extends TestKit(ActorSystem("akkaQuickstart"))
      with AnyWordSpecLike {
//    with AsyncWordSpecLike {
implicit val ec: ExecutionContext = system.dispatcher

  "build simple graph" in {
    //format: OFF
    //#simple-graph-dsl
    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val in = Source(1 to 10)
      val out = Sink.ignore

      val bcast = builder.add(Broadcast[Int](2))
      val merge = builder.add(Merge[Int](2))

      val f1, f2, f3, f4 = Flow[Int].map(_ + 10)

      in ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> out
      bcast ~> f4 ~> merge
      ClosedShape
    })
    //#simple-graph-dsl
    //format: ON

    //#simple-graph-run
    g.run()
    //#simple-graph-run
  }

  "flow connection errors" in {
    intercept[IllegalStateException] {
      //#simple-graph
      RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        val source1 = Source(1 to 10)
        val source2 = Source(1 to 10)

        val zip = builder.add(Zip[Int, Int]())

        source1 ~> zip.in0
        source2 ~> zip.in1
        // unconnected zip.out (!) => "must have at least 1 outgoing edge"
        ClosedShape
      })
      //#simple-graph
    }.getMessage should include("ZipWith2.out")
  }

  "reusing a flow in a graph" in {
    //#graph-dsl-reusing-a-flow

    val topHeadSink = Sink.head[Int]
    val bottomHeadSink = Sink.head[Int]
    val sharedDoubler = Flow[Int].map(_ * 2)

    //#graph-dsl-reusing-a-flow

    // format: OFF
    val g =
    //#graph-dsl-reusing-a-flow
      RunnableGraph.fromGraph(GraphDSL.create(topHeadSink, bottomHeadSink)((_, _)) { implicit builder =>
        (topHS, bottomHS) =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[Int](2))
          Source.single(1) ~> broadcast.in

          broadcast ~> sharedDoubler ~> topHS.in
          broadcast ~> sharedDoubler ~> bottomHS.in
          ClosedShape
      })
    //#graph-dsl-reusing-a-flow
    // format: ON
    val (topFuture, bottomFuture) = g.run()
    Await.result(topFuture, 300.millis) shouldEqual 2
    Await.result(bottomFuture, 300.millis) shouldEqual 2
  }

  "building a reusable component" in {

    //#graph-dsl-components-shape
    // A shape represents the input and output ports of a reusable
    // processing module
    case class PriorityWorkerPoolShape[In, Out](jobsIn: Inlet[In], priorityJobsIn: Inlet[In], resultsOut: Outlet[Out])
      extends Shape {

      // It is important to provide the list of all input and output
      // ports with a stable order. Duplicates are not allowed.
      override val inlets: immutable.Seq[Inlet[_]] =
      jobsIn :: priorityJobsIn :: Nil
      override val outlets: immutable.Seq[Outlet[_]] =
        resultsOut :: Nil

      // A Shape must be able to create a copy of itself. Basically
      // it means a new instance with copies of the ports
      override def deepCopy() =
        PriorityWorkerPoolShape(jobsIn.carbonCopy(), priorityJobsIn.carbonCopy(), resultsOut.carbonCopy())

    }
    //#graph-dsl-components-shape

    //#graph-dsl-components-create
    object PriorityWorkerPool {
      def apply[In, Out](
                          worker: Flow[In, Out, Any],
                          workerCount: Int): Graph[PriorityWorkerPoolShape[In, Out], NotUsed] = {

        GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._

          val priorityMerge = b.add(MergePreferred[In](1))
          val balance = b.add(Balance[In](workerCount))
          val resultsMerge = b.add(Merge[Out](workerCount))

          // After merging priority and ordinary jobs, we feed them to the balancer
          priorityMerge ~> balance

          // Wire up each of the outputs of the balancer to a worker flow
          // then merge them back
          for (i <- 0 until workerCount)
            balance.out(i) ~> worker ~> resultsMerge.in(i)

          // We now expose the input ports of the priorityMerge and the output
          // of the resultsMerge as our PriorityWorkerPool ports
          // -- all neatly wrapped in our domain specific Shape
          PriorityWorkerPoolShape(
            jobsIn = priorityMerge.in(0),
            priorityJobsIn = priorityMerge.preferred,
            resultsOut = resultsMerge.out)
        }

      }

    }
    //#graph-dsl-components-create

    def println(s: Any): Unit = ()

    //#graph-dsl-components-use
    val worker1 = Flow[String].map("step 1 " + _)
    val worker2 = Flow[String].map("step 2 " + _)

    RunnableGraph
      .fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val priorityPool1 = b.add(PriorityWorkerPool(worker1, 4))
        val priorityPool2 = b.add(PriorityWorkerPool(worker2, 2))

        Source(1 to 100).map("job: " + _) ~> priorityPool1.jobsIn
        Source(1 to 100).map("priority job: " + _) ~> priorityPool1.priorityJobsIn

        priorityPool1.resultsOut ~> priorityPool2.jobsIn
        Source(1 to 100).map("one-step, priority " + _) ~> priorityPool2.priorityJobsIn

        priorityPool2.resultsOut ~> Sink.foreach(println)
        ClosedShape
      })
      .run()
    //#graph-dsl-components-use

    //#graph-dsl-components-shape2
    import FanInShape.{Init, Name}

    class PriorityWorkerPoolShape2[In, Out](_init: Init[Out] = Name("PriorityWorkerPool"))
      extends FanInShape[Out](_init) {
      protected override def construct(i: Init[Out]) = new PriorityWorkerPoolShape2(i)

      val jobsIn = newInlet[In]("jobsIn")
      val priorityJobsIn = newInlet[In]("priorityJobsIn")
      // Outlet[Out] with name "out" is automatically created
    }
    //#graph-dsl-components-shape2

  }

  "access to materialized value" in {
    //#graph-dsl-matvalue
    import GraphDSL.Implicits._
    val foldFlow: Flow[Int, Int, Future[Int]] = Flow.fromGraph(GraphDSL.create(Sink.fold[Int, Int](0)(_ + _)) {
      implicit builder => fold =>
        FlowShape(fold.in, builder.materializedValue.mapAsync(4)(identity).outlet)
    })
    //#graph-dsl-matvalue

    Await.result(Source(1 to 10).via(foldFlow).runWith(Sink.head), 3.seconds) should ===(55)

    //#graph-dsl-matvalue-cycle
    import GraphDSL.Implicits._
    // This cannot produce any value:
    val cyclicFold: Source[Int, Future[Int]] =
      Source.fromGraph(GraphDSL.create(Sink.fold[Int, Int](0)(_ + _)) { implicit builder => fold =>
        // - Fold cannot complete until its upstream mapAsync completes
        // - mapAsync cannot complete until the materialized Future produced by
        //   fold completes
        // As a result this Source will never emit anything, and its materialited
        // Future will never complete
        builder.materializedValue.mapAsync(4)(identity) ~> fold
        SourceShape(builder.materializedValue.mapAsync(4)(identity).outlet)
      })
    //#graph-dsl-matvalue-cycle
  }
  "build with open ports" in {
    //#simple-partial-graph-dsl
    val pickMaxOfThree = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip1 = b.add(ZipWith[Int, Int, Int](math.max _))
      val zip2 = b.add(ZipWith[Int, Int, Int](math.max _))
      zip1.out ~> zip2.in0

      UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
    }

    val resultSink = Sink.head[Int]

    val g = RunnableGraph.fromGraph(GraphDSL.create(resultSink) { implicit b => sink =>
      import GraphDSL.Implicits._

      // importing the partial graph will return its shape (inlets & outlets)
      val pm3 = b.add(pickMaxOfThree)

      Source.single(1) ~> pm3.in(0)
      Source.single(2) ~> pm3.in(1)
      Source.single(3) ~> pm3.in(2)
      pm3.out ~> sink.in
      ClosedShape
    })

    val max: Future[Int] = g.run()
    Await.result(max, 300.millis) should equal(3)
    //#simple-partial-graph-dsl
  }

  "build source from partial graph" in {
    //#source-from-partial-graph-dsl
    val pairs = Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val zip = b.add(Zip[Int, Int]())
      def ints = Source.fromIterator(() => Iterator.from(1))

      // connect the graph
      ints.filter(_ % 2 != 0) ~> zip.in0
      ints.filter(_ % 2 == 0) ~> zip.in1

      // expose port
      SourceShape(zip.out)
    })

    val firstPair: Future[(Int, Int)] = pairs.runWith(Sink.head)
    //#source-from-partial-graph-dsl
    Await.result(firstPair, 300.millis) should equal(1 -> 2)
  }

  "build flow from partial graph" in {
    //#flow-from-partial-graph-dsl
    val pairUpWithToString =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        // prepare graph elements
        val broadcast = b.add(Broadcast[Int](2))
        val zip = b.add(Zip[Int, String]())

        // connect the graph
        broadcast.out(0).map(identity) ~> zip.in0
        broadcast.out(1).map(_.toString) ~> zip.in1

        // expose ports
        FlowShape(broadcast.in, zip.out)
      })

    //#flow-from-partial-graph-dsl

    // format: OFF
    val (_, matSink: Future[(Int, String)]) =
    //#flow-from-partial-graph-dsl
      pairUpWithToString.runWith(Source(List(1)), Sink.head)
    //#flow-from-partial-graph-dsl
    // format: ON

    Await.result(matSink, 300.millis) should equal(1 -> "1")
  }

  "combine sources with simplified API" in {
    //#source-combine
    val sourceOne = Source(List(1))
    val sourceTwo = Source(List(2))
    val merged = Source.combine(sourceOne, sourceTwo)(Merge(_))

    val mergedResult: Future[Int] = merged.runWith(Sink.fold(0)(_ + _))
    //#source-combine
    Await.result(mergedResult, 300.millis) should equal(3)
  }

  "combine sinks with simplified API" in {
    val actorRef: ActorRef = testActor
    //#sink-combine
    val sendRmotely = Sink.actorRef(actorRef, "Done", _ => "Failed")
    val localProcessing = Sink.foreach[Int](_ => /* do something useful */ ())

    val sink = Sink.combine(sendRmotely, localProcessing)(Broadcast[Int](_))

    Source(List(0, 1, 2)).runWith(sink)
    //#sink-combine
    expectMsg(0)
    expectMsg(1)
    expectMsg(2)
  }

}
