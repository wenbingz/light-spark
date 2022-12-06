package org.lighspark
package core

import scala.collection.mutable


class Block(private val rddId: Int, private val index: Int, val data: Seq[Any]) extends Serializable {
  final def id = Block.getId(rddId, index)
}
object Block {
  def getId(rddId: Int, index: Int) = {
    "block_" + rddId + "_" + index
  }
}

// TODO LRU cache and memory usage estimation
class BlockManager {
  var blockMapping = new mutable.HashMap[String, Seq[String]]
  var localCache = new mutable.HashMap[String, Block]
  def isBlockCached(blockId: String): Boolean = {
    localCache.synchronized {
      localCache.contains(blockId)
    }
  }
  def queryBlockLocation(blockId: String): Option[Seq[String]] = {
    blockMapping.synchronized {
      blockMapping.get(blockId)
    }
  }
  def addBlockLocation(blockId: String, executorRef: String): Unit = {
    blockMapping.synchronized {
      if (blockMapping.contains(blockId)) {
        blockMapping(blockId).:+(executorRef)
      } else {
        blockMapping += (blockId -> Seq(executorRef))
      }
    }
  }
  def getBlock(blockId: String): Option[Block] = {
    localCache.synchronized {
      localCache.get(blockId)
    }
  }
  def addBlock(block: Block): Unit = {
    localCache.synchronized {
      localCache += block.id -> block
    }
  }
  def removeCache(blockId: String) = localCache.synchronized { localCache.remove(blockId) }
}
