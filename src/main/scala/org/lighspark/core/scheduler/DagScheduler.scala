package org.lighspark.core.scheduler

import org.apache.log4j.Logger
import org.lighspark.core.SparkContext
import org.lighspark.core.rdd.Dependency
import org.lighspark.core.{SparkContext, SparkEnv}
import org.lighspark.core.rdd.{Dependency, RDD}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// TODO cut into stages and add task retry
class DagScheduler(private val sc: SparkContext) {
  val taskId = new AtomicInteger(0)
  val completedTask = new mutable.HashSet[Int]()
  val resultTask = new mutable.HashSet[Int]()
  val runningTask = new mutable.HashSet[Int]()
  val taskSetQueue = new mutable.Queue[Seq[Task[_]]]
  val taskSetStack = new mutable.Stack[Seq[Task[_]]]
  val parsedTasks = new mutable.HashSet[Task[_]] // to deduplicate tasks
  var dependencies: Seq[Dependency[_]] = _
  var results = new ArrayBuffer[Any]()
  val log = Logger.getLogger(this.getClass)


  def completeATask(task: Task[_], res: Any): Unit = synchronized {
    log.info("task " + task.taskId + " completed")
    task match {
      case rt: ResultTask[_, _] => {
        log.info("it is a result task " + res)
        results.append(res)
      }
      case _ => {

      }
    }
    runningTask.remove(task.taskId)
    completedTask.add(task.taskId)
  }

  def submitTasks(): Unit = {
    val tasks = taskSetStack.pop()
    tasks.map{
      task => {
        log.info("submitting task " + task.taskId)
        SparkEnv.sendTask(task)
        runningTask.add(task.taskId)
      }
    }
  }

  def deduplicateAndPutIntoTaskSetStack(): Unit = {
    taskSetQueue.map(taskSet => {
      taskSet.filter(t => {
        if(parsedTasks.contains(t))
          false
        else {
          parsedTasks.add(t)
          true
        }
      })
    }).map {
      taskSet => taskSetStack.push(taskSet)
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
    val tasks = partitions.map { p => new ResultTask(rdd, rdd.getPartitions()(p), taskId.getAndIncrement(), func).asInstanceOf[Task[Any]] }
    taskSetQueue.enqueue(tasks)
    dependencies = rdd.getDependencies()
    parseTasks()
    deduplicateAndPutIntoTaskSetStack()
    log.info(parsedTasks.size + " task(s) generated. ready to submit")
    while (taskSetStack.nonEmpty) {
      submitTasks()
      waitForTaskSetFinished()
    }
    for (r <- results) {
      resultHandler(0, r.asInstanceOf[U])
    }
    cleanJobStatus()
  }

  def cleanJobStatus() = {
    this.completedTask.clear()
    this.resultTask.clear()
    this.runningTask.clear()
    this.taskSetQueue.clear()
    this.taskSetStack.clear()
    this.parsedTasks.clear()
    this.dependencies = Nil
    this.results.clear()
  }
}
