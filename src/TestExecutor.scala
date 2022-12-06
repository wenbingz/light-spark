import org.lighspark.core.{BlockManager, SparkEnv}

object TestExecutor {
  def main(args: Array[String]) = {
    val executorThread = SparkEnv.initialize(null, new BlockManager, false, 5, 18888, "DriverActor", "localhost")
    new Thread() {
      SparkEnv.taskScheduler.scheduleLoop(1)
    }.start()
    Thread.sleep(1000 * 60)
  }
}
