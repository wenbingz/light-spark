package org.lighspark.executor

import org.lighspark.core.{Block, BlockManager, SparkEnv}
import org.lighspark.core.scheduler.{ResultTask, Task}

import scala.collection.mutable



class TaskScheduler(blockManager: BlockManager) {
  var queue = SparkEnv.queue

  def schedule(task: Task[Any]): Option[Any] = {
    task match {
      case rt: ResultTask[_, _] => Some(rt.result())
      case _ => {
        val res = task.execute()
        val b = new Block(task.rdd.id, task.split.index, res)
        blockManager.addBlock(b)
        SparkEnv.reportBlock(blockId = b.id)
        Some("succ")
      }
    }

  }

  def scheduleLoop(intervalInMs: Long): Unit = {
    while(true) {
      if (queue.nonEmpty) {
        val task = queue.dequeue()
        val scheduleResult = schedule(task)
        if(scheduleResult.isDefined) {
          SparkEnv.reportSuccessfulTask(task, scheduleResult.get)
        }
      }
      Thread.sleep(intervalInMs)
    }
  }
}
