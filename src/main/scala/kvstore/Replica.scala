package kvstore

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import scala.concurrent.duration._
import kvstore.Arbiter._
import kvstore.Persistence.{Persisted, Persist}
import kvstore.Replicator.{Replicated, SnapshotAck, Snapshot, Replicate}
import akka.pattern.ask
import akka.pattern.pipe

import scala.concurrent.Future

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

  implicit val replicateAndPersistTimeout: Timeout = Timeout(100 millis)
  import context.dispatcher
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persistence = context.actorOf(persistenceProps)

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def replicateToSecondaries(key:String, value:Option[String], id:Long): Iterable[Future[Replicated]] =
    secondaries map {
      case (replica:ActorRef, replicator:ActorRef) =>
        (replicator ? Replicate(key,value,id)).mapTo[Replicated]
  }

  def replicateCurrentValues(replicator: ActorRef): Unit = {
    var id:Long=0
    kv foreach { case (k:String,v:String)=>
      replicator ! Replicate(k, Option(v),id)
      id = id + 1
    }
    //TODO should I wait for  Replicated(key, id) confirmation(s)?
  }

  def persist(op: Operation, value:Option[String]) = (persistence ? Persist(op.key, value, op.id)).mapTo[Persisted] map { persisted:Persisted =>
    replicateToSecondaries(op.key,value, op.id) // TODO check if should wait for Replicated ack
    OperationAck(op.id)
  } recover{ case t:Throwable =>
    println(s"[persist] failed with: $t")
    OperationFailed(op.id)}

  /* TODO Behavior for  the leader role.
      'Clients and The KV Protocol' section, respecting the consistency guarantees described in �Guarantees for clients contacting the primary replica�.*/
  val leader: Receive = {
    case op @ Insert(key, value, id) =>
      val requestor = sender()
      kv = kv.updated(key, value)
      persist(op, Option(value)) pipeTo requestor
    case op@Remove(key, id) =>
      val requestor = sender()
      kv = kv - key
      persist(op, None) pipeTo requestor
    case op@Get(key, id)=>
      sender() ! GetResult(key, kv.get(key), id)
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
        val replicator = secondaries(removedReplica)
        secondaries = secondaries - removedReplica
        replicators = replicators - replicator
        context.stop(replicator)
        context.stop(removedReplica)
      }

    case _ =>
  }

  var expectedSnapshotSeq:Long = 0

  def updateKV(key: String, valueOption: Option[String]) = {
    kv = valueOption map { v =>
      println(s"[updateKV] : updating $key -> $v")
      kv + (key -> v)
    } getOrElse {
      println(s"[updateKV] : removing key: $key ")
      kv - key
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      println(s"[get]($key) = ${kv.get(key)}")
      sender() ! GetResult(key, kv.get(key), id)
      //TODO respect the guarantees described in �Guarantees for clients contacting the secondary replica�.
      // TODO accept replication events - Replication protocol
    case s@Snapshot(key, valueOption, seq) => //Replicator signals new state of a key
      val requestor = sender()
      seq - expectedSnapshotSeq match {
        case x if x > 0 => println(s"[snapshot] $s is not in sequence: expecting $expectedSnapshotSeq. ignoring...")
        case x if x == 0 =>
          updateKV(key, valueOption)
          println(s"[snapshot] $s in sequence. persisting...")
          (persistence ? Persist(key, valueOption, seq)).mapTo[Persisted] map { persisted: Persisted =>
            //replicateToSecondaries(key, valueOption, seq) // TODO check if should wait for Replicated ack
            SnapshotAck(key, seq)
          } pipeTo requestor
          expectedSnapshotSeq = Math.max(expectedSnapshotSeq, seq+1)
        case x if x < 0 =>
          println(s"[snapshot] $s is outdated, ignoring")
          sender() ! SnapshotAck(key, seq)
      }

    case x => println(s"[replica] unhandled: $x")
  }

}

