package org.lighspark
package core

import core.rdd.{Partition, RDD}

class Task[T](private val rdd: RDD[T], private val split: Partition) {
  def execute(): Iterator[T] = {
    rdd.compute(split)
  }
}
