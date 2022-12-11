package org.lighspark.core.rdd

import org.lighspark.core.partition.Partition
import org.lighspark.core.SparkEnv
import org.lighspark.core.partition.{Partition, Partitioner}

import scala.collection.mutable
import scala.reflect.ClassTag

class ShuffledRDDPartition(private val rddId: Int, private val splitId: Int) extends Partition {
  override val index: Int = splitId
}

// TODO a very trivial implementation without shuffle map and shuffle read and the complexity is very high, should be optimized
class ShuffledRDD[K: ClassTag, V: ClassTag](prev: RDD[(K, V)], val partitionSize: Int = SparkEnv.defaultShufflePartition, partitioner: Partitioner) extends RDD[(K, V)](prev.context, Nil) {
//  partitioner = new HashKeyPartitioner(partitionSize)
  var dependencies: Seq[Dependency[_]] = null
  private var keyOrdering: Option[Ordering[K]] = None
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  override def getDependencies() = {
   if (dependencies == null) {
     dependencies = Seq(new FullDependency[(K, V)](prev))
   }
    dependencies
  }

  override def compute(split: Partition): Iterator[(K, V)] = {
    val result = new mutable.ArrayBuffer[(K, V)]
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
                          result.append(t)
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
    // TODO use implicit
    if (keyOrdering.isDefined) {
      result.sortBy(a => a._1)(keyOrdering.get).toIterator
    } else {
      result.toIterator
    }
  }

  override def getPartitions(): Array[Partition] = (0 until partitionSize).map{
    i => new ShuffledRDDPartition(id, i)
  }.toArray
}
