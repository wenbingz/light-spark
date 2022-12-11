package org.lighspark.core.scheduler

import org.lighspark.core.partition.Partition
import org.lighspark.core.rdd.RDD

import scala.reflect.ClassTag

class Task[T: ClassTag](val rdd: RDD[T], val split: Partition, val taskId: Int) extends Serializable {
  def execute(): Iterator[T] = {
    rdd.getOrCompute(split)
  }

  override def hashCode(): Int = rdd.id * 10000 + split.index
  override def equals(obj: Any) = {
    if (obj.isInstanceOf[Task[_]]) {
      obj.hashCode() == this.hashCode()
    } else {
      false
    }
  }
}

class ResultTask[T: ClassTag, U: ClassTag](private val _rdd: RDD[T], val _split: Partition, val _taskId: Int, private val merge: (Iterator[T]) => U) extends Task[T](_rdd, _split, _taskId) {
  def result(): U = {
    val t = execute()
    merge(t)
  }
}
