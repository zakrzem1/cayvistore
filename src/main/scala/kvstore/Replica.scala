package kvstore

import akka.actor.{Actor, ActorRef, Props}
import kvstore.Arbiter._
import kvstore.Replicator.{SnapshotAck, Snapshot, Replicate}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def replicateToSecondaries(op: Operation): Unit = secondaries foreach { case (secReplica, replicator)=>

  }

  def replicateCurrentValues(replicator: ActorRef): Unit = {
    var id:Long=0
    kv foreach { case (k:String,v:String)=>
      replicator ! Replicate(k, Option(v),id)
      id = id + 1
    }
    //TODO should I wait for  Replicated(key, id) confirmation(s)?
  }

  /* TODO Behavior for  the leader role.
    'Clients and The KV Protocol' section, respecting the consistency guarantees described in �Guarantees for clients contacting the primary replica�.*/
  val leader: Receive = {
    case op @ Insert(key, value, id) =>
      kv = kv.updated(key, value)
      replicateToSecondaries(op)
    case op@Remove(key, id) =>
    case op@Get(key, id)=>
    case fromArbiter:Replicas=>
      val added = fromArbiter.replicas -- secondaries.keySet
      added.foreach { addedReplica =>
        val addedReplicator = context.actorOf(Replicator.props(addedReplica))
        replicators = replicators + addedReplicator
        secondaries = secondaries.updated(addedReplica, addedReplicator)
        replicateCurrentValues(addedReplicator)
      }
      val removed = secondaries.keySet -- fromArbiter.replicas
      removed.foreach { removedReplica =>
        secondaries = secondaries - removedReplica
        replicators = secondaries.get(removedReplica).map(replicators - _).getOrElse(replicators)
      }

    case _ =>
  }

  var expectedSnapshotSeq:Long = 0
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
      //TODO respect the guarantees described in �Guarantees for clients contacting the secondary replica�.

      // TODO accept replication events - Replication protocol
    case s@Snapshot(key, valueOption, seq) => //Replicator signals new state of a key
      seq - expectedSnapshotSeq match {
        case x if x > 0 => println(s"$s is not in sequence: $expectedSnapshotSeq. ignoring...")
        case x if x == 0 =>
          kv = valueOption map { v => kv.updated(key, v) } getOrElse (kv - key)
          sender() ! SnapshotAck(key, seq)
        case x if x < 0 => sender() ! SnapshotAck(key, seq)
      }
      expectedSnapshotSeq = Math.max(expectedSnapshotSeq, seq+1)
    case _ =>
  }

}

