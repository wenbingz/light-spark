package org.lighspark
package core.rdd

import scala.reflect.ClassTag

abstract class Dependency[T: ClassTag](private val r: RDD[T]) extends Serializable {
  def rdd = r
}
