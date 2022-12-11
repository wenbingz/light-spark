package org.lighspark.core.rdd

import org.lighspark.core.SparkContext
import org.lighspark.core.partition.Partition
import org.lighspark.core.{Block, SparkContext, SparkEnv}
import org.lighspark.core.partition.{Partition, Partitioner}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class RDD[T: ClassTag](@transient private val sc: SparkContext, @transient private var dependencies: Seq[Dependency[_]]) extends Serializable {

  def clearDependencies = dependencies = null
  def compute(split: Partition): Iterator[T]
  def getDependencies(): Seq[Dependency[_]] = dependencies
  def getPartitions(): Array[Partition]


  val id = sc.newRddId
  var partitioner: Partitioner = _
  def getOrCompute(split: Partition): Iterator[T] = {
    val blockId = Block.getId(id, split.index)
    if (SparkEnv.blockManager.isBlockCached(blockId)) {
      SparkEnv.blockManager.getBlock(blockId).get.data.asInstanceOf[Array[T]].toIterator
    } else {
      SparkEnv.getBlockLocation(blockId) match {
        case Some(actorRefs) => SparkEnv.getBlock(blockId, actorRefs.head).get.data.asInstanceOf[Array[T]].toIterator
        case None => compute(split)
      }
    }
  }
  def context = sc
  def this(@transient oneParent: RDD[_]) = {
    this(oneParent.sc, Seq(new One2OneDependency(oneParent)))
  }
  final def iterator(split: Partition): Iterator[T] = {
    getOrCompute(split)
  }

  def sortBy[K](func: T => K, ascending: Boolean = false, numPartition: Int = this.getPartitions().length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = {
    this.map(t => (func(t), t))
      .sortByKey()
      .values()
  }
  def groupBy[K](func: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    this.map(t => (func(t), t)).groupByKey()
  }

  def map[U: ClassTag](f: T => U): RDD[U] = {
    new MappedRDD[U, T](this, (_, iter) => iter.map(f))
  }

  def mapPartitionWithIndex[U: ClassTag](func: (Int, Iterator[T]) => Iterator[U], preservePartitioner: Boolean = false): RDD[U] = {
    new MappedRDD[U, T](this, func, preservePartitioner)
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
    sc.dagScheduler.runJob(this, this.getPartitions().indices, reducePartition, mergeResult)
    jobResult.getOrElse(throw new RuntimeException("do not capture valid final result"))
  }

  def collect(): Array[T] = {
    val jobResult = new ArrayBuffer[T]
    val mergeResult = (_: Int, taskResult: Option[Array[T]]) => {
      if (taskResult.isDefined) {
        jobResult.appendAll(taskResult.get)
      }
    }
    sc.dagScheduler.runJob(this, this.getPartitions().indices, (a: Iterator[T]) => Some(a.toArray), mergeResult)
    jobResult.toArray
  }
}

object RDD {
  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T]) : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)]) (implicit kt: ClassTag[K], vt: ClassTag[V]): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions[K, V] = {
    new OrderedRDDFunctions[K, V](rdd)
  }
}
