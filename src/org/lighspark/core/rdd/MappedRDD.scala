package org.lighspark.core.rdd

import org.lighspark.core.partition.Partition

import scala.reflect.ClassTag

class MappedRDDPartition(private val rddId: Int, private val splitId: Int) extends Partition {
  override val index: Int = splitId
}

class MappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], func: (Int, Iterator[T]) => Iterator[U]) extends RDD[U](prev) {
  override def compute(split: Partition): Iterator[U] = {
    func(split.index, prev.iterator(prev.getPartitions()(split.index)))
  }

  override def getPartitions(): Array[Partition] = {(0 until prev.getPartitions().size).map(i => new MappedRDDPartition(id, i)).toArray}
}
