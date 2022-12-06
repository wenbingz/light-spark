package org.lighspark
package core.rdd

import org.lighspark.core.{Block, SparkContext, SparkEnv}
import org.lighspark.core.partition.{HashPartitioner, Partition}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SequencePartition[T: ClassTag](private val rddId: Int, private val splitId: Int, val data: Seq[T]) extends Partition {
  override val index: Int = splitId
}
class SequenceRDD[T: ClassTag](@transient private val sc: SparkContext, private val data: Seq[T], private val splits: Int)
  extends RDD[T](sc, Nil) {

  override def compute(split: Partition): Iterator[T] = {
    split.asInstanceOf[SequencePartition[T]].data.iterator
  }


  private def splitSeq(): Seq[Seq[T]] = {
    val arr = new Array[ArrayBuffer[T]](splits)
    for (i <- 0 until splits) {
      arr(i) = new ArrayBuffer[T]
    }
    for (d <- data) {
      arr(partitioner.getPartition(d)).append(d)
    }
    arr
  }

  partitioner = new HashPartitioner(splits)

  var partitions: Array[Partition] = null
  override def getPartitions(): Array[Partition] = {
    if (partitions != null) {
      partitions
    } else {
      val slices = splitSeq()
      slices.indices.map(i => {
        val block = new Block(rddId = this.id, index = i, data = slices(i))
        SparkEnv.blockManager.addBlock(block)
        SparkEnv.reportBlock(block.id)
      })
      partitions = slices.indices.map(i => new SequencePartition[T](id, i, slices(i))).toArray
      partitions
    }
  }

}

