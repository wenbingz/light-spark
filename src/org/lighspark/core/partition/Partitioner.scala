package org.lighspark.core.partition

import org.lighspark.core.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

abstract class Partitioner extends Serializable {
  def numPartition: Int

  def getPartition(key: Any): Int
}

class HashPartitioner(val splits: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.hashCode() % numPartition

  override def numPartition: Int = splits
}


class HashKeyPartitioner(val splits: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    key match {
      case s: (_, _) => s._1.hashCode() % numPartition
      case _ => key.hashCode() % numPartition
    }
  }

  override def numPartition: Int = splits
}

class RangePartitioner[K: Ordering : ClassTag, V: ClassTag](val splits: Int, rdd: RDD[_ <: Product2[K, V]], private var ascending: Boolean = true) extends Partitioner {
  private var bounds: Array[K] = new Array[K](splits - 1)
  private var len: Int = 0
  override def numPartition: Int = splits
  val random = new Random(System.currentTimeMillis())

  def getBounds() = {
    for (p <- rdd.getPartitions()) {
      val iter = rdd.iterator(p)
      while (iter.hasNext) {
        if (len < splits - 1) {
          bounds(len) = iter.next()._1
          len += 1
        } else {
          val r = random.nextInt(splits)
          if (r < splits - 1) {
            bounds(r) = iter.next()._1
          }
        }
      }
    }
  }
  override def getPartition(key: Any): Int = {
    getBounds()
    0
  }
}