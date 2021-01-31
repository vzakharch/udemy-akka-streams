package part2primer

import akka.actor.ActorSystem

object OperatorFusion extends App {
  implicit val system: ActorSystem = ActorSystem("OperatorFusion")
  import system.dispatcher


}
