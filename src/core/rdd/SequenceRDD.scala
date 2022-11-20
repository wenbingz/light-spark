package org.lighspark
package core.rdd

private class SequencePartition(private val splitId: Int, private val idx: Int) extends Partition {
  override val index: Int = idx
}
class SequenceRDD[T](private val id : Long, private val data: Seq[T]) extends RDD[T](id, data) {



  override def compute(split: Partition): Iterator[T] = ???

  override def getDependencies(): Seq[Dependency[_]] = ???

  override def getPartitions(): Seq[Partition] = ???
}
