package org.lighspark.core.rdd

import org.lighspark.core.partition.RangePartitioner

import scala.reflect.ClassTag

class OrderedRDDFunctions[K: Ordering: ClassTag, V: ClassTag](private val self: RDD[(K, V)]) extends Serializable {
  private val ordering = implicitly[Ordering[K]]
  def sortByKey(ascending: Boolean = true, numPartition: Int = self.getPartitions().length): RDD[(K, V)] = {
    val partitioner = new RangePartitioner(numPartition, self)
    new ShuffledRDD[K, V](self, numPartition, partitioner)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
}
