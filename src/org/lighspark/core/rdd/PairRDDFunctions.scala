package org.lighspark.core.rdd


import org.lighspark.core.SparkEnv

import scala.reflect.ClassTag

class PairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) extends Serializable {
  def groupByKey(): RDD[(K, Iterable[V])] = {
    new ShuffledRDD[K, V](self, SparkEnv.defaultShufflePartition)
  }
}
