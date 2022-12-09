## What is light spark
just very simple implementation of spark to help beginners understand how Spark framework works

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
```bash
run with program entrypoint in src/TestDriver.scala
run with program entrypoint in src/TestExecutor.scala
```
Driver output:
```bash
(abc,3)
(abcd,1)
(ab,2)
(ac,1)
(bc,2)
(cd,1)
```
and it is the result of 
```scala
val res = sparkContext.parallelize(Seq("abc", "ab", "abc", "bc", "ac", "abc", "cd", "abcd", "ab", "bc"), 3)
  .map(a => (a, 1))
  .groupBy(_._1)
  .map(a => (a._1, a._2.map(b => b._2).sum))
  .collect()
res.map(a => println(a))
```
just like Apache Spark