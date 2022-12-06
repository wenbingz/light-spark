import org.lighspark.core.SparkContext

import org.lighspark.core.rdd.RDD
import org.lighspark.core.rdd.DoubleRDDFunctions

object TestDriver {
  def main(args: Array[String]) = {
    val sparkContext = new SparkContext
    Thread.sleep(1000 * 10)
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    val res1 = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6), 3).map(t => {
      (t % 2, t)
    }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(res1)
    val res2 = sparkContext.parallelize[Int](Seq(1, 2, 3, 4, 5, 6), 3).groupBy(a => {
      a % 2
    }).reduce((a, b) => {
      (a._1 + b._1, a._2 ++ b._2)
    })
    println(res2)
//    val res = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7), 3).min
    val res3 = sparkContext.parallelize(Seq("abc", "ab", "abc", "bc", "ac", "abc", "cd", "abcd", "ab", "bc"), 3)
      .map(a => (a, 1))
      .groupBy(_._1)
      .map(a => (a._1, a._2.map(b => b._2).sum))
      .collect()
    res3.map(a => println(a))


//    print(res)
    System.exit(0)
  }
}
