package org.lighspark.core

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.pattern.ask
import akka.remote.transport.ActorTransportAdapter.AskTimeout
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.lighspark.core.scheduler.DagScheduler
import org.lighspark.rpc.InternalGetBlock
import org.lighspark.core.scheduler.{DagScheduler, Task}
import org.lighspark.executor.TaskScheduler
import org.lighspark.rpc.{BlockLocation, GetBlock, HeartBeat, InternalGetBlock, QueryBlock, RegisterExecutor, RegisteredExecutor, ReportBlock, SendBlock, SendExecutorTask, SendHeartBeat, SendTask, TaskComplete}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.util.Random

case class ExecutorInfo(actorUrl: String)
class SparkEnv(dagScheduler: DagScheduler, blockManager: BlockManager, syncInvokeTimeout: Int, driverUrl: String) extends Actor {
  @transient val log = Logger.getLogger(this.getClass)
  override def preStart(): Unit = {
    if (driverUrl != null) {
      SparkEnv.driverRef = context.actorSelection(driverUrl)
      SparkEnv.driverRef ! RegisterExecutor(1, 1)
    }
  }

  override def receive: Receive = {
    case RegisterExecutor(cores, memoryInMB) => {
      val executorId = SparkEnv.id.addAndGet(1)
      SparkEnv.id2Executor += (executorId -> ExecutorInfo(sender().path.toString))
      sender() ! RegisteredExecutor(executorId)
    }
    case QueryBlock(blockId) => {
      sender() ! BlockLocation(blockId, blockManager.queryBlockLocation(blockId))
    }
    case ReportBlock(blockId, actorRef) => {
      log.info("block report from " + actorRef + " of block " + blockId)
      blockManager.addBlockLocation(blockId, actorRef)
      sender() ! "succ"
    }
    case GetBlock(blockId) => {
      log.info(sender().path + " requests block " + blockId)
      sender() ! SendBlock(blockManager.getBlock(blockId))
    }
    case HeartBeat(executorId) => {
      // TODO remove executor when heartbeat timeout
      log.info("heart beat from " + executorId)
    }
    case TaskComplete(task, result) => {
      dagScheduler.completeATask(task, result)
    }

    case RegisteredExecutor(executorId: Int) => {
      SparkEnv.executorId = executorId
      log.info("registered with executor Id " + executorId)
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
      log.info("trying to get block " + blockId + " from " + actorUrl)
      val res = actorRef ? GetBlock(blockId)
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
  var executorUrl: String = null
  var executorId: Int = _
  var actorRef: ActorRef = _
  var executorRef: ActorRef = _
  val id2Executor = new mutable.HashMap[Int, ExecutorInfo]()
  val id: AtomicInteger = new AtomicInteger(0)
  var taskScheduler: TaskScheduler = _
  var driverRef: ActorSelection = _
  var dagScheduler: DagScheduler = _
  val defaultShufflePartition = 10
  val defaultTaskSchedulerIntervalInMS = (0.5 * 1000).toLong
  val random = new Random(System.currentTimeMillis())
  val log = Logger.getLogger(this.getClass)
  def createDriver(): SparkEnv = {
    new SparkEnv(dagScheduler, blockManager, syncInvokeTimeout, null)
  }

  def  createExecutor(): SparkEnv = {
    new SparkEnv(dagScheduler, blockManager, syncInvokeTimeout, driverUrl)
  }

  def initialize(dagScheduler: DagScheduler, blockManager: BlockManager, isDriver: Boolean, syncInvokeTimeout: Int, driverPort: Int, driverActorName: String, driverHost: String, executorPort: Int, executorActorName: String, executorHost: String): Thread = {
    this.isDriver = isDriver
    this.blockManager = blockManager
    this.syncInvokeTimeout = syncInvokeTimeout
    this.driverUrl = "akka.tcp://DriverActorSystem@" + driverHost + ":" + driverPort +  "/user/" + driverActorName
    this.executorUrl = "akka.tcp://ExecutorActorSystem@" + executorHost + ":" + executorPort +  "/user/" + executorActorName
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
          |akka.remote.netty.tcp.hostname = %s
          |akka.remote.netty.tcp.port = %d
          |""".format(executorHost, executorPort).stripMargin
      )
      val executorActorSystem = ActorSystem("ExecutorActorSystem", config)
      val actorRef = executorActorSystem.actorOf(Props(createExecutor()), executorActorName)
      SparkEnv.executorRef = actorRef
      taskScheduler = new TaskScheduler(blockManager)
      taskScheduler.start()
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
      val future = Await.result((actorRef ? InternalGetBlock(blockId, actorUrl)).mapTo[Future[SendBlock]], Timeout(syncInvokeTimeout seconds).duration)
      val res = Await.result(future, Timeout(SparkEnv.syncInvokeTimeout second).duration)
      res.block
    } else {
      val future = Await.result((executorRef ? InternalGetBlock(blockId, actorUrl)).mapTo[Future[SendBlock]], Timeout(syncInvokeTimeout seconds).duration)
      val res = Await.result(future, Timeout(SparkEnv.syncInvokeTimeout second).duration)
      res.block
    }
  }

  def reportBlock(blockId: String) = {
    if (isDriver) {
      Await.result((actorRef ? ReportBlock(blockId, driverUrl)).mapTo[String], Timeout(syncInvokeTimeout seconds).duration)
    } else {
      Await.result((driverRef ? ReportBlock(blockId, executorUrl)).mapTo[String], Timeout(syncInvokeTimeout seconds).duration)
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
        log.info("no executor to send task " + task.taskId)
        return false
      } else {
        // random pick executor
        val executorIds = id2Executor.keySet.toArray
        val picked = random.nextInt(executorIds.length)
        val actorUrl = id2Executor(executorIds(picked)).actorUrl
        log.info("try to send task " + task.taskId + " to " + actorUrl)
        actorRef ! SendExecutorTask(task, actorUrl)
        return true
      }
    }
    true
  }
}
