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

  def this(@transient oneParent: RDD[_]) = {
    this(oneParent.sc, Seq(new NarrowDependency(oneParent)))
  }
  final def iterator(split: Partition): Iterator[T] = {
    getOrCompute(split)
  }

  def map[U: ClassTag](f: T => U): RDD[U] = {
    new MappedRDD[U, T](this, (_, iter) => iter.map(f))
  }

  def reduce(func: (T, T) => T): T = {
    val reducePartition: Iterator[T] => Option[T] =  iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(func))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    val mergeResult =(_: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(func(value, taskResult.get))
          case None => taskResult
        }
      }
    }
    sc.dagScheduler.runJob(this, this.getPartitions().map{p => p.index}, reducePartition, mergeResult)
    jobResult.getOrElse(throw new RuntimeException("do not capture valid final result"))
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
