package org.lighspark.core.scheduler

import org.lighspark.core.rdd.RDD

import scala.actors.threadpool.AtomicInteger

class DagScheduler {
  val jobId = new AtomicInteger(0)
  def runJob[T, U](
        rdd: RDD[T],
        partitions: Seq[Int],
        func: (Iterator[T]) => U,
        resultHandler: (Int, U) => Unit
      ): Unit = {

  }

}
