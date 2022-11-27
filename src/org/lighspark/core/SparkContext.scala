package org.lighspark
package core

import org.lighspark.core.rdd.{RDD, SequenceRDD}
import org.lighspark.driver.Driver

import scala.actors.threadpool.AtomicInteger
import scala.reflect.ClassTag

class SparkContext {
  @transient var rddId: AtomicInteger = new AtomicInteger()
  @transient var taskId: AtomicInteger = new AtomicInteger()

  val blockManager = new BlockManager

  @transient var rpcEndpoint: Driver = new Driver(blockManager, 5)

  new Thread() {
    Driver.main(rpcEndpoint)
  }.start()

  def newRddId: Int = {
    rddId.getAndIncrement()
  }

  def newTaskId: Int = {
    taskId.getAndIncrement()
  }



  def parallelize[T: ClassTag](data: Seq[T], numSplits: Int):  SequenceRDD[T] = {
    new SequenceRDD[T](this, data, numSplits)
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (Iterator[T]) => Iterator[U],
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Array[T] = {
    null
  }
}
