package org.lighspark.executor

import org.lighspark.core.scheduler.ResultTask
import org.lighspark.core.BlockManager
import org.lighspark.core.{Block, BlockManager, SparkEnv}
import org.lighspark.core.scheduler.{ResultTask, Task}


class TaskScheduler(blockManager: BlockManager) extends Thread {
  var queue = SparkEnv.queue

  def scheduleLoop(intervalInMs: Long): Unit = {
    while (true) {
      if (!queue.isEmpty) {
        val task = queue.poll()
        val scheduleResult = schedule(task)
        if (scheduleResult.isDefined) {
          SparkEnv.reportSuccessfulTask(task, scheduleResult.get)
        }
      }
      Thread.sleep(intervalInMs)
    }
  }


  def schedule(task: Task[Any]): Option[Any] = {
    task match {
      case rt: ResultTask[_, _] => Some(rt.result())
      case _ => {
        val res = task.execute()
        val b = new Block(task.rdd.id, task.split.index, res.toArray)
        blockManager.addBlock(b)
        SparkEnv.reportBlock(blockId = b.id)
        Some("succ")
      }
    }
  }
  override def run() = {
    scheduleLoop(SparkEnv.defaultTaskSchedulerIntervalInMS)
  }
}
