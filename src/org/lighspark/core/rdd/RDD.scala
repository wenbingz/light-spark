package org.lighspark
package core.rdd
import core.{Block, SparkContext, SparkEnv}
import org.lighspark.core.partition.{Partition, Partitioner}
import org.lighspark.core.scheduler.Task

import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](@transient private val sc: SparkContext, @transient private var dependencies: Seq[Dependency[_]]) extends Serializable {
  def clearDependencies = dependencies = null
  def compute(split: Partition): Iterator[T]
  def getDependencies(): Seq[Dependency[_]] = dependencies
  def getPartitions(): Seq[Partition]

  val id = sc.newRddId
  var partitioner: Partitioner = _
  def getOrCompute(split: Partition): Iterator[T] = {
    val blockId = Block.getId(id, split.index)
    if (SparkEnv.blockManager.isBlockCached(blockId)) {
      SparkEnv.blockManager.getBlock(blockId).get.data.asInstanceOf[Iterator[T]]
    } else {
      SparkEnv.getBlockLocation(blockId) match {
        case Some(actorRefs) => SparkEnv.getBlock(blockId, actorRefs.head).get.data.asInstanceOf[Iterator[T]]
        case None => compute(split)
      }
    }
  }

  final def iterator(split: Partition): Iterator[T] = {
    getOrCompute(split)
  }

  def map[U: ClassTag](f: T => U): RDD[U] = {
    null
  }

  def parallel[T: ClassTag](data: Seq[T]): Unit = {
    val rdd = new SequenceRDD[T](sc, data, 3)

    val task = new Task[T](rdd, rdd.getPartitions().head, 1)
    SparkEnv.sendTask(task)
  }

  def collect(): Array[T] = {
    null
//    for (p <- getPartitions()) {
//
//    }
  }
}
