package org.lighspark.core.partition

import org.lighspark.core.rdd.RDD
import org.lighspark.utils.SamplingUtils

import scala.reflect.ClassTag

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

class RangePartitioner[K: Ordering : ClassTag, V: ClassTag](val splits: Int, rdd: RDD[_ <: Product2[K, V]], private var ascending: Boolean = true, private val sampleSizePerPartition: Int = 20) extends Partitioner {

    val bounds: Array[K] = {
    val sampleSize = math.min(splits * sampleSizePerPartition, 1e6)
    val sampleSizePerOriginalPartition = math.ceil(3.0d * sampleSize / rdd.getPartitions().length).toInt
    val sampleFromPartition = rdd.map(_._1).mapPartitionWithIndex((idx, p) => {
      val (sampled, cnt) = SamplingUtils.reservoirSampleAndCount(p, sampleSizePerOriginalPartition)
      Iterator((idx, sampled, cnt))
    }).collect()
    SamplingUtils.mapSamplingResultToBounds(splits, sampleFromPartition)
  }

  override def numPartition: Int = bounds.length + 1
  override def getPartition(key: Any): Int = {
    val partition = SamplingUtils.binarySearch(bounds, key.asInstanceOf[K]) + 1
    if (ascending) {
      partition
    } else {
      bounds.length - partition
    }
  }
}