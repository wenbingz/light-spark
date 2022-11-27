package org.lighspark.core.partition

abstract class Partitioner extends Serializable {
  def numPartition: Int
  def getPartition(key: Any): Int
}

class HashPartitioner(val splits: Int) extends Partitioner {
  override def numPartition: Int = splits

  override def getPartition(key: Any): Int = key.hashCode() % numPartition
}

