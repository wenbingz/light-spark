package org.lighspark
package core

import scala.actors.threadpool.AtomicInteger

class SparkContext {
  @transient var rddId: AtomicInteger = new AtomicInteger()
  def newRddId: Int = {
    rddId.addAndGet(1)
  }
}
