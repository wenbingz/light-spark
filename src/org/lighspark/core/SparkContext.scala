package org.lighspark
package core

import org.lighspark.core.rdd.{RDD, SequenceRDD}
import org.lighspark.core.scheduler.{DagScheduler, Task}

import scala.actors.threadpool.AtomicInteger
import scala.reflect.ClassTag

class SparkContext {
  @transient var rddId: AtomicInteger = new AtomicInteger()
  @transient var taskId: AtomicInteger = new AtomicInteger()
  @transient val blockManager = new BlockManager
  @transient val dagScheduler = new DagScheduler(this)
  @transient var thread = SparkEnv.initialize(dagScheduler, blockManager, true, 5, null)


  def newRddId: Int = {
    rddId.getAndIncrement()
  }

  def newTaskId: Int = {
    taskId.getAndIncrement()
  }

  def parallelize[T: ClassTag](data: Seq[T], numSplits: Int):  SequenceRDD[T] = {
    val rdd = new SequenceRDD[T](this, data, numSplits)
    rdd
  }

  def runJob[T: ClassTag, U: ClassTag] (
      rdd: RDD[T],
      func: (Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit) = {
    dagScheduler.runJob[T, U](rdd, partitions, func, resultHandler)
  }
}
