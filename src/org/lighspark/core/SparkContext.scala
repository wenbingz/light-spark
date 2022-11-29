package org.lighspark
package core

import org.lighspark.core.rdd.{RDD, SequenceRDD}
import org.lighspark.core.scheduler.Task

import scala.actors.threadpool.AtomicInteger
import scala.reflect.ClassTag

class SparkContext {
  @transient var rddId: AtomicInteger = new AtomicInteger()
  @transient var taskId: AtomicInteger = new AtomicInteger()
  @transient val blockManager = new BlockManager
  @transient var thread = SparkEnv.initialize(blockManager, true, 5, null)

  def newRddId: Int = {
    rddId.getAndIncrement()
  }

  def newTaskId: Int = {
    taskId.getAndIncrement()
  }



  def parallelize[T: ClassTag](data: Seq[T], numSplits: Int):  SequenceRDD[T] = {
    val rdd = new SequenceRDD[T](this, data, numSplits)
    val task = new Task[T](rdd, rdd.getPartitions().head, 33)
    SparkEnv.sendTask(task)
    rdd
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (Iterator[T]) => Iterator[U],
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Array[T] = {
    null
  }
}
