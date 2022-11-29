package org.lighspark.core

import org.lighspark.core.rdd.SequenceRDD
import org.lighspark.core.scheduler.Task

object Test {
  def main(args: Array[String]) = {
//    val rdd = new SequenceRDD(new SparkContext, Seq(1, 3, 4, 2, 1, 3, 4, 5, 6, 3, 2, 3), 3)
//    val task = new Task(rdd, rdd.getPartitions().head, 1)
//    val res = task.execute()
//    while (res.hasNext) {
//      print(res.next() + " ")
//    }
    val sparkContext = new SparkContext
    Thread.sleep(1000 * 5)
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    sparkContext.parallelize[Int](Seq(1, 3, 4, 2, 1, 3, 4, 5, 6, 3, 2, 3), 3)
    Thread.sleep(1000 * 60)
  }
}
