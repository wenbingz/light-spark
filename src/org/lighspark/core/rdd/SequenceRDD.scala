package org.lighspark
package core.rdd

import org.lighspark.core.SparkContext
import org.lighspark.core.partition.{HashPartitioner, Partition}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SequencePartition[T: ClassTag](private val rddId: Int, private val splitId: Int, val data: Seq[T]) extends Partition {
  override val index: Int = splitId
}
class SequenceRDD[T: ClassTag](private val sc: SparkContext, private val data: Seq[T], private val splits: Int)
  extends RDD[T](sc, Nil) {

  override def compute(split: Partition): Iterator[T] = {
    split.asInstanceOf[SequencePartition[T]].data.iterator
  }


  private def splitSeq(): Seq[Seq[T]] = {
    val arr = new Array[ArrayBuffer[T]](splits)
    for (d <- data) {
      if (arr(partitioner.getPartition(d)) == null) {
        arr(partitioner.getPartition(d)) = new ArrayBuffer[T]
      }
      arr(partitioner.getPartition(d)).append(d)
    }
    arr
  }

  partitioner = new HashPartitioner(splits)

  var partitions: Seq[SequencePartition[T]] = null
  override def getPartitions(): Seq[Partition] = {
    if (partitions != null) {
      partitions
    } else {
      val slices = splitSeq()
      partitions = slices.indices.map(i => new SequencePartition[T](id, i, slices(i))).seq
      partitions
    }
  }

}

