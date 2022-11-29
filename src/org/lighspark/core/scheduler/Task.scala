package org.lighspark.core.scheduler

import org.lighspark.core.partition.Partition
import org.lighspark.core.rdd.RDD

import scala.reflect.ClassTag

class Task[T: ClassTag](val rdd: RDD[T], val split: Partition, val taskId: Int) extends Serializable {
  def execute(): Iterator[T] = {
    rdd.getOrCompute(split)
  }
}
