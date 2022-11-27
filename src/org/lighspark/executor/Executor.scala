package org.lighspark
package executor

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.pattern.ask
import akka.remote.transport.ActorTransportAdapter.AskTimeout
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.lighspark.core.scheduler.Task
import org.lighspark.core.{Block, BlockManager}
import org.lighspark.rpc.{BlockLocation, GetBlock, HeartBeat, QueryBlock, RegisterExecutor, RegisteredExecutor, ReportBlock, SendBlock, SendHeartBeat, SendTask, TaskComplete}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class Executor(blockManager: BlockManager, syncInvokeTimeout: Int, driverUrl: String) extends Actor {
  var executorId: Int = _
  var driverRef: ActorSelection = _
  var actorRef: ActorRef = _
  var queue = new mutable.Queue[Task[Any]]
  override def preStart(): Unit = {
    driverRef = context.actorSelection(driverUrl)
    driverRef ! RegisterExecutor(1, 1)
  }

  override def receive: Receive = {
    case RegisteredExecutor(executorId: Int) => {
      this.executorId = executorId
      print("registered with executor Id " + executorId)
    }
    case SendTask(task) => {
      queue.synchronized {
        queue.enqueue(task)
      }
    }
    case SendHeartBeat => {
      driverRef ! HeartBeat(this.executorId)
    }
  }

  def getBlock(blockId: String, actorUrl: String): Option[Block] = {
    val actorRef = context.actorSelection(actorUrl)
    val res = Await.result((actorRef ? GetBlock(blockId)).mapTo[SendBlock], Timeout(syncInvokeTimeout seconds).duration)
    res.block
  }

  def reportBlock(blockId: String) = {
    Await.result((driverRef ? ReportBlock(blockId)), Timeout(syncInvokeTimeout seconds).duration)
  }

  def getBlockLocation(blockId: String): Option[Seq[String]] = {
    val res = Await.result((driverRef ? QueryBlock(blockId)).mapTo[BlockLocation], Timeout(syncInvokeTimeout seconds).duration)
    res.actorRef
  }

  def reportSuccessfulTask(taskId: Int): Unit = {
    driverRef ! TaskComplete(taskId)
  }

  def initialize(): Unit = {
    val config = ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = localhost
        |""".stripMargin
    )
    val executorActorSystem = ActorSystem("ExecutorActorSystem", config)
    actorRef = executorActorSystem.actorOf(Props(new Executor(new BlockManager, 5, "akka.tcp://DriverActorSystem@localhost:18888/user/DriverActor")), "ExecutorActor")
  }
  def heartBeat(): Unit = {
    var cnt = 1
    while (true) {
      actorRef ! SendHeartBeat
      cnt = cnt + 1
      Thread.sleep(3000)
    }
  }
}