package org.lighspark.executor

import org.lighspark.core.{Block, BlockManager, SparkEnv}
import org.lighspark.core.scheduler.Task

import scala.collection.mutable



class TaskScheduler(blockManager: BlockManager) {
  var queue = SparkEnv.queue

  def schedule(task: Task[Any]): Boolean = {
    val res = task.execute()
    for (r <- res) {
      println(r)
    }
    val b = new Block(task.rdd.id, task.split.index, res)
    blockManager.addBlock(b)
    SparkEnv.reportBlock(blockId = b.id)
    true
  }

  def scheduleLoop(intervalInMs: Long): Unit = {
    while(true) {
      if (queue.nonEmpty) {
        val task = queue.dequeue()
        if(schedule(task)) {
          SparkEnv.reportSuccessfulTask(task.taskId)
        }
      }
      Thread.sleep(intervalInMs)
    }
  }
}
