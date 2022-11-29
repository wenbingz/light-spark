import org.lighspark.core.{BlockManager, SparkEnv}

object TestExecutor {
  def main(args: Array[String]) = {
    val executorThread = SparkEnv.initialize(null, new BlockManager, false, 5, "akka.tcp://DriverActorSystem@localhost:18888/user/DriverActor")
    new Thread() {
      SparkEnv.taskScheduler.scheduleLoop(1)
    }.start()
    Thread.sleep(1000 * 60)
  }
}
