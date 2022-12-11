package org.lighspark.core

import org.apache.log4j.Logger
import org.lighspark.core.rdd.SequenceRDD
import org.lighspark.core.rdd.{RDD, SequenceRDD}
import org.lighspark.core.scheduler.DagScheduler

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class SparkContext {
  @transient var rddId: AtomicInteger = new AtomicInteger()
  @transient var taskId: AtomicInteger = new AtomicInteger()
  @transient val blockManager = new BlockManager
  @transient val dagScheduler = new DagScheduler(this)
  @transient var thread = SparkEnv.initialize(dagScheduler, blockManager, true, 5, 18888, "DriverActor", "localhost", -1, null, null)
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