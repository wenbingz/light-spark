package org.lighspark.driver

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.remote.transport.ActorTransportAdapter.AskTimeout
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.lighspark.core.scheduler.Task
import org.lighspark.core.{Block, BlockManager}
import org.lighspark.rpc.{BlockLocation, GetBlock, HeartBeat, QueryBlock, RegisterExecutor, RegisteredExecutor, ReportBlock, SendBlock, SendTask, TaskComplete}

import scala.actors.threadpool.AtomicInteger
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

case class ExecutorInfo(actorUrl: String)
class Driver(val blockManager: BlockManager, syncInvokeTimeout: Int) extends Actor {
  val id2Executor = new mutable.HashMap[Int, ExecutorInfo]()
  val id: AtomicInteger = new AtomicInteger(0)

  def getBlock(blockId: String, actorUrl: String): Option[Block] = {
    val actorRef = context.actorSelection(actorUrl)
    val res = Await.result((actorRef ? GetBlock(blockId)).mapTo[SendBlock], Timeout(syncInvokeTimeout seconds).duration)
    res.block
  }

  def sendTask(task: Task[Any]): Boolean = {
    synchronized(id2Executor) {
      if (id2Executor.isEmpty) {
        return false
      } else {
        val actorUrl = id2Executor.head._2.actorUrl
        println("try to send task " + task.taskId + " to " + actorUrl)
        val actorRef = context.actorSelection(actorUrl)
        actorRef ! SendTask(task)
        return true
      }
    }
    true
  }

  override def receive: Receive = {
    case RegisterExecutor(cores, memoryInMB) => {
      println(sender().path)
      val executorId = id.addAndGet(1)
      id2Executor += (executorId -> ExecutorInfo(sender().path.toString))
      sender() ! RegisteredExecutor(executorId)
    }
    case QueryBlock(blockId) => {
      sender() ! BlockLocation(blockId, blockManager.queryBlockLocation(blockId))
    }
    case ReportBlock(blockId) => {
      blockManager.addBlockLocation(blockId, sender().path.toString)
    }
    case GetBlock(blockId) => {
      sender() ! SendBlock(blockManager.getBlock(blockId))
    }
    case HeartBeat(executorId) => {
      println("heart beat from " + executorId)
    }
    case TaskComplete(taskId) => {
      println(taskId + " has been completed successfully!")
    }
  }

}

object Driver {
  def main(driverInstance: Driver): Unit = {
    val config = ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = localhost
        |akka.remote.netty.tcp.port = 18888
        |""".stripMargin
    )
    val driverActorSystem = ActorSystem("DriverActorSystem", config)
    driverActorSystem.actorOf(Props(driverInstance), "DriverActor")
  }
}
