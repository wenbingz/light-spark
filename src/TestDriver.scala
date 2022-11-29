import org.lighspark.core.SparkContext

object TestDriver {
  def main(args: Array[String]) = {
    val sparkContext = new SparkContext
    Thread.sleep(1000 * 5)
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    val res = sparkContext.parallelize[Int](Seq(1, 3, 4, 2, 1, 3, 4, 5, 6, 3, 2, 3), 3).map(a => a - 1).reduce((a, b) => a + b)
    println(res)
    System.exit(0)
  }
}
