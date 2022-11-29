## What is light spark
just very simple implementation of spark to help learner understand how Spark framework works

## All the Spark core concepts included
- RDD
- Dag scheduler
- Block Manager
- akka as messaging protocol

## It can really be run in distributed mode

## Test
```bash
run with program entrypoint in src/TestDriver.scala
run with program entrypoint in src/TestExecutor.scala
```
Driver output:
```bash
task 0 completed
Some(13) ------------------------------------------
task 1 completed
Some(6) ------------------------------------------
task 2 completed
Some(6) ------------------------------------------
akka.tcp://ExecutorActorSystem@localhost:2552/user/ExecutorActor
heart beat from 1
submitting task 3
try to send task 3 to akka.tcp://ExecutorActorSystem@localhost:2552/user/ExecutorActor
submitting task 4
try to send task 4 to akka.tcp://ExecutorActorSystem@localhost:2552/user/ExecutorActor
submitting task 5
try to send task 5 to akka.tcp://ExecutorActorSystem@localhost:2552/user/ExecutorActor
task 3 completed
task 4 completed
task 5 completed
25
```
and it is the result of 
```scala
sparkContext.parallelize[Int](Seq(1, 3, 4, 2, 1, 3, 4, 5, 6, 3, 2, 3), 3).map(a => a - 1).reduce((a, b) => a + b)
```
just like Apache Spark