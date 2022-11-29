package org.lighspark.core.rdd

import org.lighspark.core.partition.Partition

import scala.reflect.ClassTag

class MappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], func: (Int, Iterator[T]) => Iterator[U]) extends RDD[U](prev) {
  override def compute(split: Partition): Iterator[U] = {
    func(split.index, prev.iterator(split))
  }

  override def getPartitions(): Seq[Partition] = prev.getPartitions()
}
