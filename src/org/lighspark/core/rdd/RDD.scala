package org.lighspark
package core.rdd
import core.SparkContext
import org.lighspark.core.partition.{Partition, Partitioner}

import scala.reflect.ClassTag

abstract class RDD[T](@transient private val sc: SparkContext, @transient private var dependencies: Seq[Dependency[_]]) extends Serializable {
  def clearDependencies = dependencies = null
  def compute(split: Partition): Iterator[T]
  def getDependencies(): Seq[Dependency[_]] = dependencies
  def getPartitions(): Seq[Partition]

  val id = sc.newRddId
  var partitioner: Partitioner = _
  def firstParent[U:ClassTag](): RDD[U]= {
    getDependencies().head.rdd.asInstanceOf[RDD[U]]
  }
  def getOrCompute(split: Partition): Iterator[T] = {
    compute(split)
  }
  final def iterator(split: Partition): Iterator[T] = {
    getOrCompute(split)
  }

  def map[U: ClassTag](f: T => U): RDD[U] = {
    null
  }

  def collect(): Array[T] = {
    null
//    for (p <- getPartitions()) {
//
//    }
  }
}
