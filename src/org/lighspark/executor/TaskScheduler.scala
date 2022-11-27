package org.lighspark.executor

import org.lighspark.core.{Block, BlockManager}
import org.lighspark.core.scheduler.Task

import scala.collection.mutable



class TaskScheduler(rpcEndpoint: Executor, parallelism: Int, blockManager: BlockManager) {
  var queue = rpcEndpoint.queue

  def schedule(task: Task[Any]): Boolean = {
    val res = task.execute()
    val b = new Block(task.rdd.id, task.split.index, res)
    blockManager.addBlock(b)
    rpcEndpoint.reportBlock(blockId = b.getId)
    true
  }

  def scheduleLoop(intervalInMs: Long): Unit = {
    while(true) {
      if (queue.nonEmpty) {
        val task = queue.dequeue()
        if(schedule(task)) {
          rpcEndpoint.reportSuccessfulTask(task.taskId)
        }
      }
      Thread.sleep(intervalInMs)
    }
  }
}
