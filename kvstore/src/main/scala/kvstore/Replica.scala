package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, Timers}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

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

  case class OperationTimedout(id: Long)
  case class OperationInfo(key: String, sender: ActorRef, pendingReplicators: Set[ActorRef]) {
    def removePending(replicators: Set[ActorRef]): OperationInfo
    = copy(pendingReplicators = this.pendingReplicators -- replicators)
  }

  case object GlobalAckCheck

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  val persistence = context.system.actorOf(persistenceProps)
  var idToOperationInfo = Map.empty[Long, OperationInfo]

  timers.startPeriodicTimer("globalAck", GlobalAckCheck, 100.milliseconds)

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  override def preStart(): Unit = { arbiter ! Join }

  def receive: Receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case insert: Insert        => handleOperationMsg(insert)
    case remove: Remove        => handleOperationMsg(remove)
    case Get(k, id)            => handleGetMsg(k, id)
    case Replicas(replicas)    => handleReplicasMsg(replicas)
    case Replicated(k, id)     => removeReplicatorFromPendingList(id)
    case GlobalAckCheck        => checkGlobalAcknowledgement()
    case Persisted(k, id)      => respondToClient(id)(OperationAck)
    case OperationTimedout(id) => respondToClient(id)(OperationFailed)
  }

  var expectedSeqNumber = 0L

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k, id)           => handleGetMsg(k, id)
    case o: Operation         => sender ! OperationFailed(o.id)
    case Snapshot(k, v, seq)  => handleSnapshotMsg(k, v, seq)
    case p: Persist           => persistence ! p
    case Persisted(k, seq)    => sendSnapshotAck(k, seq)
  }


  def handleOperationMsg(op: Operation): Unit = {
    val (key, value, id) = (op: @unchecked) match {
      case Insert(k, v, i) =>
        kv += (k -> v)
        (k, Some(v), i)
      case Remove(k, i) =>
        kv -= k
        (k, None, i)
    }
    idToOperationInfo += (id -> OperationInfo(key, sender, replicators))
    if (replicators.nonEmpty) {
      replicators.foreach(_ ! Replicate(key, value, id))
      timers.startSingleTimer(id, OperationTimedout(id), 1.second)
    } else {
      timers.startPeriodicTimer(s"sendPersist$id", Persist(key, kv.get(key), id), 100.milliseconds)
    }
    timers.startSingleTimer(id, OperationTimedout(id), 1.second)
  }

  def handleReplicasMsg(replicas: Set[ActorRef]): Unit = {
    val newReplicators = replicas.collect { case r if r != self =>
      secondaries.getOrElse(r, {
        val replicator = context.system.actorOf(Replicator.props(r))
        secondaries += (r -> replicator)
        replicator
      })
    }

    val removedReplicators = replicators -- newReplicators
    replicators = newReplicators

    removedReplicators.foreach(_ ! PoisonPill)
    idToOperationInfo = idToOperationInfo.map { case (k, v) => (k, v.removePending(removedReplicators)) }

    for {
      replicator <- newReplicators
      (k, v) <- kv
    } {
      replicator ! Replicate(k, Some(v), -1)
    }
  }

  def removeReplicatorFromPendingList(id: Long): Unit =
    idToOperationInfo.get(id).foreach { opInfo =>
      idToOperationInfo += (id -> opInfo.removePending(Set(sender)))
    }

  def checkGlobalAcknowledgement(): Unit =
    idToOperationInfo.foreach { case (id, OperationInfo(k, s, rs)) =>
      if (rs.isEmpty) { persistence ! Persist(k, kv.get(k), id) }
    }

  def respondToClient(id: Long)(msg: Long => OperationReply): Unit = {
    timers.cancel(id)
    timers.cancel(s"sendPersist$id")
    idToOperationInfo(id).sender ! msg(id)
    idToOperationInfo -= id
  }

  def handleGetMsg(key: String, id: Long): Unit = sender ! GetResult(key, kv.get(key), id)

  def handleSnapshotMsg(key: String, valueOption: Option[String], seq: Long): Unit = {
    secondaries += (self -> sender)
    if (seq == expectedSeqNumber) {
      valueOption match {
        case Some(value) => kv += (key -> value)
        case None => kv -= key
      }
      expectedSeqNumber += 1
      timers.startPeriodicTimer(s"sendPersist$seq", Persist(key, valueOption, seq), 100.milliseconds)
    } else if (seq < expectedSeqNumber) {
      expectedSeqNumber = (seq + 1) max expectedSeqNumber
      timers.startPeriodicTimer(s"sendPersist$seq", Persist(key, valueOption, seq), 100.milliseconds)
    }
  }

  def sendSnapshotAck(key: String, seq: Long): Unit = {
    timers.cancel(s"sendPersist$seq")
    secondaries(self) ! SnapshotAck(key, seq)
  }
}

