package kvstore

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.duration._

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator.
    *
    * TODO The sender reference when sending the Snapshot message must be the Replicator actor (not the primary replica actor or any other).
    * */
  def receive: Receive = {
    case r@Replicate(key, valueOpt, id) =>
      val replicateInitiator = sender()
      implicit val timeout: Timeout = Timeout(100 millis)
      (replica ? Snapshot(key, valueOpt, nextSeq)).mapTo[SnapshotAck].map { snapAck =>
        Replicated(r.key, r.id)
      } pipeTo replicateInitiator
    case x => println(s"[Replicator] Unhandled msg $x")
  }

}
