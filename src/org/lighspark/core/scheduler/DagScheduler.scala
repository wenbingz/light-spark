package org.lighspark.core.scheduler

import org.lighspark.core.{SparkContext, SparkEnv}
import org.lighspark.core.rdd.{Dependency, RDD}

import scala.actors.threadpool.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class DagScheduler(private val sc: SparkContext) {
  val taskId = new AtomicInteger(0)
  val completedTask = new mutable.HashSet[Int]()
  val resultTask = new mutable.HashSet[Int]()
  val runningTask = new mutable.HashSet[Int]()
  val taskSetQueue = new mutable.Queue[Seq[Task[_]]]
  var dependencies: Seq[Dependency[_]] = _
  var results = new ArrayBuffer[Any]()


  def completeATask(task: Task[_], res: Any): Unit = synchronized {
    println("task " + task.taskId + " completed")
    task match {
      case rt: ResultTask[_, _] => {
        results.append(res)
      }
      case _ => {

      }
    }
    runningTask.remove(task.taskId)
    completedTask.add(task.taskId)
  }

  def submitTasks(): Unit = {
    val tasks = taskSetQueue.dequeue()
    tasks.map{
      task => {
        println("submitting task " + task.taskId)
        SparkEnv.sendTask(task)
        runningTask.add(task.taskId)
      }
    }
  }
  def parseTasks(): Unit = {
    if (dependencies == Nil || dependencies.isEmpty) {
      return
    }
    val tasks = dependencies.flatMap {
      dependency => dependency.rdd.getPartitions().map {
        p => new Task(dependency.rdd, p, taskId.getAndIncrement())
      }
    }
    dependencies = tasks.filter(task => task.rdd.getDependencies() != Nil).flatMap(task => task.rdd.getDependencies())
    taskSetQueue.enqueue(tasks)
    parseTasks()
  }

  def waitForTaskSetFinished(): Unit = {
    while (runningTask.nonEmpty) {
      Thread.sleep(1000 * 2)
    }
  }

  def runJob[T: ClassTag, U: ClassTag](
        rdd: RDD[T],
        partitions: Seq[Int],
        func: (Iterator[T]) => U,
        resultHandler: (Int, U) => Unit
      ) = {
    val tasks = rdd.getPartitions().map { p => new ResultTask(rdd, p, taskId.getAndIncrement(), func).asInstanceOf[Task[Any]] }
    taskSetQueue.enqueue(tasks)
    dependencies = rdd.getDependencies()
    parseTasks()
    while (taskSetQueue.nonEmpty) {
      submitTasks()
      waitForTaskSetFinished()
    }
    for (r <- results) {
      resultHandler(0, r.asInstanceOf[U])
    }
  }

}
