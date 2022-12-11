## What is light spark
just a very simple implementation of Spark to help beginners understand how Spark framework works

## All the Spark core concepts included
- RDD
- Dag scheduler (without stage split)
- Block Manager (all cached in memory without LRU)
- Shuffle (naive implementation)
- Operator: `map`, `mapPartition`, `reduce`, `groupBy`, `sortBy`, `collect`
- Partitioner: `HashPartitioner`, `RangePartitioner`
- akka as messaging protocol

## It can really be run in distributed mode

## Test
### Run driver and executor in single process
```bash
run with program entrypoint in TestDriver.scala

mvn clean package
java -jar spark-1.0-SNAPSHOT.jar
```
output:
```bash
test result:
(1,ArrayBuffer(6, 4, 2, 3, 1, 5))
 ****************************
1.0
 ****************************
(abc,3) (abcd,1) (ab,2) (ac,1) (bc,2) (cd,1)
****************************
2 2 2 3 3 3 3 3 4 4 5 5 5 5 6 6 23 34 232 234 754
```
and it is the result of 
```scala
val res1 = sc.parallelize[Int](Seq(1, 2, 3, 4, 5, 6), 3)
  .groupBy(a => a % 2)
  .reduce((a, b) => {
    (a._1 + b._1, a._2 ++ b._2)
  })
// Test case 2: min
val res2 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7), 3).min
// Test case 3: collect
val res3 = sc.parallelize(
  Seq("abc", "ab", "abc", "bc", "ac", "abc", "cd", "abcd", "ab", "bc"), 3)
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
```
### Run in Fully Distributed mode
#### Driver source code
```scala
import org.lighspark.core.{SparkContext, SparkEnv}

object TestSpark {
  def main(args: Array[String]) = {
    val sc = new SparkContext
    // wait for executors to join in
    Thread.sleep(1000 * 10)
    val res = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7), 3).min
    print(res)
  }
}
```
#### Executor source code
```scala
import org.lighspark.core.{SparkContext, BlockManager, SparkEnv}

object Executor {
  val executorThread = SparkEnv.initialize(null, new BlockManager, false, 5, 18888, "DriverActor", "localhost", 22552, "ExecutorActor", "localhost")
  Thread.sleep(1000 * 1000)
}
```
