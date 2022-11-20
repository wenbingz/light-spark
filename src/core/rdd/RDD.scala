package org.lighspark
package core.rdd
import core.SparkContext

import scala.reflect.ClassTag

abstract class RDD[T](private val sc: SparkContext, private var dependencies: Seq[Dependency[_]]) extends Serializable {
  def clearDependencies = dependencies = null
  def compute(split: Partition): Iterator[T]
  def getDependencies(): Seq[Dependency[_]]
  def getPartitions(): Seq[Partition]
  def firstParent[U:ClassTag](): RDD[U]= {
    getDependencies().head.rdd.asInstanceOf[RDD[U]]
  }
  def getOrCompute(split: Partition): Iterator[T] = {
  }
  final def iterator(split: Partition): Iterator[T] = {
    getOrCompute(split)
  }
}
