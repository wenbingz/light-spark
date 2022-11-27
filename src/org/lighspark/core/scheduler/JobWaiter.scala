package org.lighspark.core.scheduler

import scala.actors.threadpool.AtomicInteger
import scala.concurrent.Promise

class JobWaiter[T](dagScheduler: DagScheduler, val jobId: Int, totalTasks: Int, resultHandler: (Int, T) => Unit) {
  private val finishedTasks = new AtomicInteger(0)
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()

  def jobCompleted = jobPromise.isCompleted

  def completionFuture = jobPromise.future

  def taskSucceeded(index: Int, result: Any): Unit =  {
    synchronized {
      resultHandler(index, result.asInstanceOf[T])
    }
    if (finishedTasks.getAndIncrement() == totalTasks) {
      jobPromise.success(())
    }
  }

  def jobFailed(exception: Exception): Unit = {
    jobPromise.tryFailure(exception)
  }

}
