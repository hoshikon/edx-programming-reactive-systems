/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.Props
import akka.testkit.TestProbe
import org.scalatest.{FunSuiteLike, Matchers}

trait IntegrationSpec
  extends FunSuiteLike
        with Matchers { this: KVStoreSuite =>

  import Arbiter._
  test("flaky persistence") {
    val audit = TestProbe()
    val arbiter = system.actorOf(Props(classOf[given.Arbiter], true, audit.ref), "flaky-arbiter")
    val primary = system.actorOf(Replica.props(arbiter, given.Persistence.props(flaky = true)), "integration-primary")
    val client = session(primary)


    client.getAndVerify("k1")
    client.setAcked("k1", "v1")
    client.getAndVerify("k1")
    client.getAndVerify("k2")
    client.setAcked("k2", "v2")
    client.getAndVerify("k2")
    client.removeAcked("k1")
    client.getAndVerify("k1")

    val secondary = system.actorOf(Replica.props(arbiter, given.Persistence.props(flaky = true)), "integration-secondary")
    val secondary2 = system.actorOf(Replica.props(arbiter, given.Persistence.props(flaky = true)), "integration-secondary-2")
    val secondary3 = system.actorOf(Replica.props(arbiter, given.Persistence.props(flaky = true)), "integration-secondary-3")
    val secondary4 = system.actorOf(Replica.props(arbiter, given.Persistence.props(flaky = true)), "integration-secondary-4")
    val secondary5 = system.actorOf(Replica.props(arbiter, given.Persistence.props(flaky = true)), "integration-secondary-5")

    val client2 = session(secondary)
    val client3 = session(secondary2)
    val client4 = session(secondary3)
    val client5 = session(secondary4)
    val client6 = session(secondary5)

    client2.getAndVerify("k1")
    client.set("k3", "v3")
    Thread.sleep(1000)
    assert(client2.get("k3").contains("v3"))
    assert(client3.get("k3").contains("v3"))
    assert(client4.get("k3").contains("v3"))
    assert(client5.get("k3").contains("v3"))
    assert(client6.get("k3").contains("v3"))
  }

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */
  }
