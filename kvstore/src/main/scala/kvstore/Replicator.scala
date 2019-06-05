package kvstore

import akka.actor.{Actor, ActorRef, Props, Timers}
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with Timers {
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
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r: Replicate        => sendSnapshotMsg(r)
    case SnapshotAck(k, seq) => sendReplicatedMsg(k, seq)
    case s: Snapshot         => replica ! s
  }

  def sendSnapshotMsg(r: Replicate): Unit = {
    val seq = nextSeq()
    acks += (seq -> ((sender, r)))
    timers.startPeriodicTimer(s"snapshot$seq", Snapshot(r.key, r.valueOption, seq), 100.milliseconds)
  }

  def sendReplicatedMsg(key: String, seq: Long): Unit = {
    timers.cancel(s"snapshot$seq")
    val (sndr, r) = acks(seq)
    sndr ! Replicated(key, r.id)
    acks -= seq
  }
}
