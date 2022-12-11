package org.lighspark

import org.lighspark.core.{BlockManager, SparkContext, SparkEnv}

object TestSpark {
  def main(args: Array[String]) = {
    val sc = new SparkContext
    Thread.sleep(1000 * 2)
    // only one executor is supported since SparkEnv can not be shared
    // between executors.
    // if you want to run multiple executors
    // run them in different JVM instances
    val executorThread = SparkEnv.initialize(null, new BlockManager, false, 5, 18888, "DriverActor", "localhost", 22552, "ExecutorActor", "localhost")
    // waiting executors to join
    Thread.sleep(1000 * 2)
    // Test case 1: reduce
    val res1 = sc.parallelize[Int](Seq(1, 2, 3, 4, 5, 6), 3)
      .groupBy(a => a % 2)
      .reduce((a, b) => {
        (a._1 + b._1, a._2 ++ b._2)
      })
    // Test case 2: min
    val res2 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7), 3).min
    // Test case 3: collect
    val res3 = sc.parallelize(
      Seq("abc", "ab", "abc", "bc", "ac", "abc", "cd", "abed", "ab", "bc"), 3)
      .map(a => (a, 1))
      .groupBy(a => a._1)
      .map(a => (a._1, a._2.map(b => b._2).sum))
      .collect()
    // Test case 4: sort
    val res4 = sc.parallelize(
      Seq(4, 3, 6, 2, 2, 232, 23, 234, 3, 4, 5,
        3, 6, 5, 3, 5, 2, 5, 754, 3, 34), 3)
      .sortBy(a => a)
      .collect()

    println("test result: ")
    println(res1)
    println(" **************************** ")
    println(res2)
    println(" **************************** ")
    res3.foreach(a => print(a + " "))
    println()
    println("****************************")
    res4.foreach(a => print(a + " "))
  }
}
