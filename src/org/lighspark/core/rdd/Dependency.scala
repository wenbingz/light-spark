package org.lighspark
package core.rdd

import org.lighspark.core.partition.Partition

import scala.reflect.ClassTag

abstract class Dependency[T: ClassTag](private val r: RDD[T]) extends Serializable {
  def rdd = r
  def getParent(partitionId: Int): Seq[Int]
}


class One2OneDependency[T: ClassTag](private val r: RDD[T]) extends Dependency[T](r) {
  override def getParent(partitionId: Int): Seq[Int] = Seq(partitionId)
}

class FullDependency[T: ClassTag](private val r: RDD[T]) extends Dependency[T](r) {
  def getParent(partitionId: Int): Seq[Int] = rdd.getPartitions().indices
}