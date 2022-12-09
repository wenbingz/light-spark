package org.lighspark.core

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.pattern.ask
import akka.remote.transport.ActorTransportAdapter.AskTimeout
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.lighspark.core.scheduler.{DagScheduler, Task}
import org.lighspark.executor.TaskScheduler
import org.lighspark.rpc.{BlockLocation, GetBlock, HeartBeat, InternalGetBlock, QueryBlock, RegisterExecutor, RegisteredExecutor, ReportBlock, SendBlock, SendExecutorTask, SendHeartBeat, SendTask, TaskComplete}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.actors.threadpool.AtomicInteger
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.util.Random

case class ExecutorInfo(actorUrl: String)
class SparkEnv(dagScheduler: DagScheduler, blockManager: BlockManager, syncInvokeTimeout: Int, driverUrl: String) extends Actor {


  override def preStart(): Unit = {
    if (driverUrl != null) {
      SparkEnv.driverRef = context.actorSelection(driverUrl)
      SparkEnv.driverRef ! RegisterExecutor(1, 1)
    }
  }

  override def receive: Receive = {
    case RegisterExecutor(cores, memoryInMB) => {
      println(sender().path)
      val executorId = SparkEnv.id.addAndGet(1)
      SparkEnv.id2Executor += (executorId -> ExecutorInfo(sender().path.toString))
      sender() ! RegisteredExecutor(executorId)
    }
    case QueryBlock(blockId) => {
      sender() ! BlockLocation(blockId, blockManager.queryBlockLocation(blockId))
    }
    case ReportBlock(blockId) => {
      if (sender().path.toString.contains("akka.tcp")) {
        blockManager.addBlockLocation(blockId, sender().path.toString)
      } else { // only driver will report block to itself
        blockManager.addBlockLocation(blockId, SparkEnv.driverUrl)
      }
      sender() ! "succ"
    }
    case GetBlock(blockId) => {
      sender() ! SendBlock(blockManager.getBlock(blockId))
    }
    case HeartBeat(executorId) => {
      // TODO remove executor when heartbeat timeout
      println("heart beat from " + executorId)
    }
    case TaskComplete(task, result) => {
      dagScheduler.completeATask(task, result)
    }

    case RegisteredExecutor(executorId: Int) => {
      SparkEnv.executorId = executorId
      println("registered with executor Id " + executorId)
    }
    case SendTask(task) => {
      SparkEnv.queue.synchronized {
        SparkEnv.queue.offer(task)
      }
    }
    case SendHeartBeat => {
      SparkEnv.driverRef ! HeartBeat(SparkEnv.executorId)
    }
    case InternalGetBlock(blockId, actorUrl) => {
      val actorRef = context.actorSelection(actorUrl)
      println("trying to get block from " + actorUrl)
      val res = Await.result((actorRef ? GetBlock(blockId)).mapTo[SendBlock], Timeout(syncInvokeTimeout seconds).duration)
      println("got block " + res.block.get.id)
      sender() ! res
    }
    case SendExecutorTask(task, actorUrl) => {
      val actorRef = context.actorSelection(actorUrl)
      actorRef ! SendTask(task)
    }
  }
}

object SparkEnv {
  var queue = new ConcurrentLinkedQueue[Task[Any]]
  var blockManager: BlockManager = _
  var rpcEndpoint: SparkEnv = _
  var isDriver: Boolean = false
  var syncInvokeTimeout: Int = _
  var driverUrl: String = null
  var executorId: Int = _
  var actorRef: ActorRef = _
  var executorRef: ActorRef = _
  val id2Executor = new mutable.HashMap[Int, ExecutorInfo]()
  val id: AtomicInteger = new AtomicInteger(0)
  var taskScheduler: TaskScheduler = _
  var driverRef: ActorSelection = _
  var dagScheduler: DagScheduler = _
  val defaultShufflePartition = 10
  def createDriver(): SparkEnv = {
    new SparkEnv(dagScheduler, blockManager, syncInvokeTimeout, null)
  }

  def  createExecutor(): SparkEnv = {
    new SparkEnv(dagScheduler, blockManager, syncInvokeTimeout, driverUrl)
  }

  def initialize(dagScheduler: DagScheduler, blockManager: BlockManager, isDriver: Boolean, syncInvokeTimeout: Int, driverPort: Int, driverActorName: String, driverHost: String): Thread = {
    this.isDriver = isDriver
    this.blockManager = blockManager
    this.syncInvokeTimeout = syncInvokeTimeout
    this.driverUrl = "akka.tcp://DriverActorSystem@" + driverHost + ":" + driverPort +  "/user/" + driverActorName
    this.dagScheduler = dagScheduler
    if (isDriver) {
      val config = ConfigFactory.parseString(
        """
          |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
          |akka.remote.netty.tcp.hostname = %s
          |akka.remote.netty.tcp.port = %d
          |""".format(driverHost, driverPort).stripMargin
      )
      val thread = new Thread() {
        override def run() = {
          val driverActorSystem = ActorSystem("DriverActorSystem", config)
          actorRef = driverActorSystem.actorOf(Props(createDriver()), driverActorName)
          Thread.sleep(1000 * 60 * 60 * 24 * 1000)
        }
      }
      thread.start()
      thread
    } else {
      val config = ConfigFactory.parseString(
        """
          |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
          |akka.remote.netty.tcp.hostname = localhost
          |""".stripMargin
      )
      val executorActorSystem = ActorSystem("ExecutorActorSystem", config)
      val actorRef = executorActorSystem.actorOf(Props(createExecutor()), "ExecutorActor")
      SparkEnv.executorRef = actorRef
      taskScheduler = new TaskScheduler(blockManager)
      val thread = new Thread() {
        override def run() = {
          while (true) {
            actorRef ! SendHeartBeat
            Thread.sleep(3000)
          }
        }
      }
      thread.start()
      thread
    }
  }

  def getBlock(blockId: String, actorUrl: String): Option[Block] = {
    if (isDriver) {
      val res = Await.result((actorRef ? InternalGetBlock(blockId, actorUrl)).mapTo[SendBlock], Timeout(syncInvokeTimeout seconds).duration)
      res.block
    } else {
      val res = Await.result((executorRef ? InternalGetBlock(blockId, actorUrl)).mapTo[SendBlock], Timeout(syncInvokeTimeout seconds).duration)
      res.block
    }
  }

  def reportBlock(blockId: String) = {
    if (isDriver) {
      Await.result((actorRef ? ReportBlock(blockId)).mapTo[String], Timeout(syncInvokeTimeout seconds).duration)
    } else {
      Await.result((driverRef ? ReportBlock(blockId)).mapTo[String], Timeout(syncInvokeTimeout seconds).duration)
    }
  }

  def getBlockLocation(blockId: String): Option[Seq[String]] = {
    if (isDriver) {
      val res = Await.result((actorRef ? QueryBlock(blockId)).mapTo[BlockLocation], Timeout(syncInvokeTimeout seconds).duration)
      res.actorRef
    } else {
      val res = Await.result((driverRef ? QueryBlock(blockId)).mapTo[BlockLocation], Timeout(syncInvokeTimeout seconds).duration)
      res.actorRef
    }
  }

  def reportSuccessfulTask(task: Task[_], result: Any): Unit = {
    driverRef ! TaskComplete(task, result: Any)
  }

  //TODO add location preference
  def sendTask[T: ClassTag](task: Task[T]): Boolean = {
    synchronized(id2Executor) {
      if (id2Executor.isEmpty) {
        println("no executor to send task " + task.taskId)
        return false
      } else {
        // random pick executor
        val executorIds = id2Executor.keySet.toArray
        val picked = new Random(System.currentTimeMillis()).nextInt(executorIds.length)
        val actorUrl = id2Executor(executorIds(picked)).actorUrl
        println("try to send task " + task.taskId + " to " + actorUrl)
        actorRef ! SendExecutorTask(task, actorUrl)
        return true
      }
    }
    true
  }
}
