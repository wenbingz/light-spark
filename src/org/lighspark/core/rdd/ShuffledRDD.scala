package org.lighspark.core.rdd

import org.lighspark.core.{SparkEnv}
import org.lighspark.core.partition.{HashKeyPartitioner, Partition}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class ShuffledRDDPartition(private val rddId: Int, private val splitId: Int) extends Partition {
  override val index: Int = splitId
}

// TODO a very trivial implementation shuffle with shuffle map and shuffle read but complexity is very high
class ShuffledRDD[K: ClassTag, V: ClassTag](prev: RDD[(K, V)], val partitionSize: Int = SparkEnv.defaultShufflePartition) extends RDD[(K, Iterable[V])](prev.context, Nil) {
  partitioner = new HashKeyPartitioner(partitionSize)

  override def getDependencies() = {
    Seq(new FullDependency[(K, V)](prev))
  }

  override def compute(split: Partition): Iterator[(K, Iterable[V])] = {
    val result = new mutable.HashMap[K, ArrayBuffer[V]]
    getDependencies().map{
      dep => {
        dep match {
          case d: FullDependency[_] => {
            d.getParent(split.index).map {
              prevSlice => {
                val iter = d.rdd.iterator(d.rdd.getPartitions()(prevSlice))
                while (iter.hasNext) {
                    iter.next() match {
                      case t: (K, V) =>
                        if (partitioner.getPartition(t._1) == split.index) {
                          if (!result.contains(t._1)) {
                            result(t._1) = new ArrayBuffer[V]
                          }
                          result(t._1).append(t._2)
                        }
                      case _ => {
                        throw new RuntimeException("the dependency rdd should only contain tuple2 element")
                      }
                    }
                  }
              }
            }
          }
          case _ => {
            throw new RuntimeException("Full dependency is expected when shuffling in current version")
          }
        }
      }
    }
    result.toIterator
  }



  override def getPartitions(): Array[Partition] = (0 until partitionSize).map{
    i => new ShuffledRDDPartition(id, i)
  }.toArray
}
