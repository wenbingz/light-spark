import org.lighspark.core.SparkContext

import scala.collection.mutable.ArrayBuffer

object TestDriver {
  def main(args: Array[String]) = {
    val sparkContext = new SparkContext
    Thread.sleep(1000 * 5)
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
//    val resultHandler = ArrayBuffer[()]
//    val res = sparkContext.parallelize[Int](Seq(1, 3, 4, 2, 1, 3, 4, 5, 6, 3, 2, 3), 3).map(a => a - 1).reduce((a, b) => a + b)
    //    val res = sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6), 3).map(t => {
//      println(t + " ----------------------------------------------------")
//      (t % 2, t)
//    }).reduce((a, b) => (a._1 + b._1, a._2 + b._2 ))
//    val res = sparkContext.parallelize[Int](Seq(1, 2, 3, 4, 5, 6), 3).groupBy(a => {
//      a % 2
//    }).reduce((a, b) => {
//      (a._1 + b._1, a._2 ++ b._2)
//    })
    val res = sparkContext.parallelize(Seq("abc", "ab", "abc", "bc", "ac", "abc", "cd", "abcd", "ab", "bc"), 3)
                          .map(a => (a, 1))
                          .groupBy(_._1)
                          .map(a => (a._1, a._2.map(b => b._2).sum))
                          .map(a => (a._1, a._2.toString))
                          .reduce((a, b) => (a._1 + "_" + b._1, a._2 + "_" + b._2)) // to trigger job submission since collect() is not realized
    println(res)
    System.exit(0)
  }
}
