package org.lighspark.core.rdd

import org.lighspark.core.partition.Partition

import scala.reflect.ClassTag


class CoalescedRDDPartition(private val rddId: Int, private val splitId: Int) extends Partition {
  override val index: Int = splitId
}

//class CoalescedRDD[T: ClassTag](var prev: RDD[T], val partitionNum: Int) extends RDD[T](prev.context, Nil) {
//  override def compute(split: Partition): Iterator[T] = ???
//
//  override def getDependencies(): Seq[Dependency[_]] = {
//
//  }
//
//  override def getPartitions(): Seq[Partition] = {
//
//  }
//}
