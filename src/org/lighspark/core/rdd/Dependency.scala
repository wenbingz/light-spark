package org.lighspark
package core.rdd

import scala.reflect.ClassTag

abstract class Dependency[T: ClassTag](private val r: RDD[T]) extends Serializable {
  def rdd = r
}

class NarrowDependency[T: ClassTag](private val r: RDD[T]) extends Dependency[T](r) {
  def getParent(partitionId: Int): Seq[Int] = Seq[Int](partitionId)
}