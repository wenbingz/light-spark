package org.lighspark
package core

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Block(private val rddId: Int, private val index: Int, val data: Iterator[Any]) extends Serializable {
  final def getId = "block_" + rddId + "_" + index;
}

class BlockManager {
  var blockMapping = new mutable.HashMap[String, Seq[String]]
  var localCache = new mutable.HashMap[String, Block]
  def isBlockCached(blockId: String): Boolean = {
    localCache.contains(blockId)
  }
  def queryBlockLocation(blockId: String): Option[Seq[String]] = {
    blockMapping.get(blockId)
  }
  def addBlockLocation(blockId: String, executorRef: String): Unit = {
    if (blockMapping.contains(blockId)) {
      blockMapping(blockId).:+(executorRef)
    } else {
      blockMapping += (blockId -> Seq(executorRef))
    }
  }
  def getBlock(blockId: String): Option[Block] = {
    localCache.get(blockId)
  }
  def addBlock(block: Block): Unit = {
    localCache += block.getId -> block
  }
  def removeCache(blockId: String) = localCache.remove(blockId)
}
