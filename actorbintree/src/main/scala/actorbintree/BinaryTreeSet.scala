/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

  case object DigestQueue
}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC => doGC()
  }

  val digesting: Receive = {
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)
    case DigestQueue =>
      if (pendingQueue.isEmpty) {
        context.become(normal)
      } else {
        val (op, rest) = pendingQueue.dequeue
        root ! op
        pendingQueue = rest
        self ! DigestQueue
      }
    case GC => doGC()

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot
      if (pendingQueue.isEmpty) {
        context.become(normal)
      } else {
        context.become(digesting)
        self ! DigestQueue
      }
    case GC => ()
  }

  private def doGC() = {
    val newRoot = createRoot
    context.become(garbageCollecting(newRoot))
    root ! CopyTo(newRoot)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(req, id, e) =>
      if (e == elem) {
        removed = false
        req ! OperationFinished(id)
      } else {
        getSubtree(e) match {
          case Some(st) =>
            st ! Insert(req, id, e)
          case None =>
            subtrees += (lOrR(e) -> context.actorOf(BinaryTreeNode.props(e, false)))
            req ! OperationFinished(id)
        }
      }

    case Contains(req, id, e) =>
      if (e == elem) {
        req ! ContainsResult(id, !removed)
      } else {
        getSubtree(e) match {
          case Some(st) => st ! Contains(req, id, e)
          case None => req ! ContainsResult(id, false)
        }
      }

    case Remove(req, id, e) =>
      if (e == elem) {
        removed = true
        req ! OperationFinished(id)
      } else {
        getSubtree(e) match {
          case Some(st) => st ! Remove(req, id, e)
          case None => req ! OperationFinished(id)
        }
      }

    case CopyTo(tree) =>
      val expected = subtrees.values.toSet + self
      context.become(copying(expected))

      if (!removed) {
        tree ! Insert(self, 0, elem)
      } else {
        self ! OperationFinished(0)
      }
      subtrees.values.foreach(_ ! CopyTo(tree))
  }

  private def lOrR(e: Int): Position = if (e < elem) Left else Right
  private def getSubtree(e: Int): Option[ActorRef] = subtrees.get(lOrR(e))

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef]): Receive = {
    case OperationFinished(_) => finishOrBecome(expected - self)
    case CopyFinished => finishOrBecome(expected - sender())
  }

  private def finishOrBecome(expected: Set[ActorRef]): Unit = {
    if (expected.isEmpty) {
      context.parent ! CopyFinished
    } else {
      context.become(copying(expected))
    }
  }

}
