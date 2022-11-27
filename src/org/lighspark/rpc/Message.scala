package org.lighspark.rpc

import org.lighspark.core.Block
import org.lighspark.core.scheduler.Task

case class HeartBeat(executorId: Int)
case class RegisterExecutor(cores: Int, memoryInMB: Int)
case class RegisteredExecutor(executorId: Int)
case object SendHeartBeat
case class SendBlock(block: Option[Block])
case class QueryBlock(blockId: String)
case class GetBlock(blockId: String)
case class ReportBlock(blockId: String)
case class BlockLocation(blockId: String, actorRef: Option[Seq[String]])
case class SendTask[Any](task: Task[Any])
case class TaskComplete(taskId: Int)
case class TaskFailed(taskId: Int)

