package org.lighspark.core

object Test2 {
  def main(args: Array[String]) = {
    //    val rdd = new SequenceRDD(new SparkContext, Seq(1, 3, 4, 2, 1, 3, 4, 5, 6, 3, 2, 3), 3)
    //    val task = new Task(rdd, rdd.getPartitions().head, 1)
    //    val res = task.execute()
    //    while (res.hasNext) {
    //      print(res.next() + " ")
    //    }
    val executorThread = SparkEnv.initialize(new BlockManager, false, 5, "akka.tcp://DriverActorSystem@localhost:18888/user/DriverActor")
    new Thread() {
      SparkEnv.taskScheduler.scheduleLoop(1)
    }.start()
    Thread.sleep(1000 * 60)
  }
}
