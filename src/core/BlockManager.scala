package org.lighspark
package core

class Block(private val rddId: Int, private val index: Int, private val data: Iterator[Any]) extends Serializable {
  final def getId = "block_" + rddId + "_" + index;
}

class BlockManager {
  var blockMapping: Map[String, String] = _
  var localCache: Map[String, Block] = _
  def queryBlockLocation(blockId: String): String = null
  def getBlock(blockId: String): Block = null
}
