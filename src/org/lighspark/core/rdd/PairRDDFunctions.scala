package org.lighspark.core.rdd


import org.lighspark.core.SparkEnv
import org.lighspark.core.partition.HashKeyPartitioner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class PairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) extends Serializable {
  def groupByKey(): RDD[(K, Iterable[V])] = {
    new ShuffledRDD[K, V](self, SparkEnv.defaultShufflePartition, new HashKeyPartitioner(SparkEnv.defaultShufflePartition)).mapPartitionWithIndex {
      (idx, iter) => {
        val hashMap = new mutable.HashMap[K, ArrayBuffer[V]]
        while (iter.hasNext) {
          val t = iter.next()
          t match {
            case (k, v) => {
              if (!hashMap.contains(k)) {
                hashMap(k) = new ArrayBuffer[V]
              }
              hashMap(k).append(v)
            }
          }
        }
        hashMap.toIterator
      }
    }
  }

  def values(): RDD[V] = {
    self.map(_._2)
  }
}
