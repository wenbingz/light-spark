package org.lighspark.utils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

object SamplingUtils {
  def reservoirSampleAndCount[T: ClassTag](input: Iterator[T], k: Int): (Array[T], Long) = {
    val arr = new Array[T](k)
    var cnt: Long = 0
    while (input.hasNext && cnt < k) {
      arr(cnt.toInt) = input.next()
      cnt = cnt + 1
    }
    if (cnt < k) {
      val trimmedArr = new Array[T](cnt.toInt)
      System.arraycopy(arr, 0, trimmedArr, 0, cnt.toInt)
      (trimmedArr, cnt)
    } else {
      val rand = new Random(System.currentTimeMillis())
      while (input.hasNext) {
        cnt += 1
        val idx = (rand.nextDouble() * cnt).toLong
        val item = input.next()
        if (idx < k) {
          arr(idx.toInt) = item
        }
      }
      (arr, cnt)
    }
  }

  def mapSamplingResultToBounds[T: Ordering: ClassTag](partitions: Int, samples: Array[(Int, Array[T], Long)]): Array[T] = {
    var candidates = new ArrayBuffer[(T, Double)]
    samples.filter(d => d._3 != 0).foreach {
      sample => {
        val weight = sample._3.toDouble / sample._2.length
        for (key <- sample._2) {
          candidates.append((key, weight))
        }
      }
    }
    val targetLen =  math.min(partitions, candidates.length)
    candidates = candidates.sortBy(_._1)
//    candidates.foreach {
//      c => {
//        print("[" + c._1 + ", " + c._2 + "]")
//      }
//    }
//    println()
    var (i, j) = (0, 0)
    var culWeight = 0.0d
    val sumWeight = candidates.map(_._2.toDouble).sum
    val step = sumWeight / targetLen
    var target = step
    val bounds = new ArrayBuffer[T]
    while (i < candidates.length && j < targetLen - 1) {
      val (key, weight) = candidates(i)
      culWeight += weight
      // println(step + " * " + culWeight + " * " + target)
      if (culWeight >= target) {
        if (bounds.isEmpty || Ordering[T].gt(key, bounds(bounds.length - 1))) {
          // println("+1")
          bounds.append(key)
          j += 1
          target += step
        }
      }
      i += 1
    }
    bounds.toArray
  }

  def binarySearch[T: Ordering: ClassTag](bounds: Array[T], l: Int, r: Int, key: T): Int = {
    if (l == r) {
      return l - 1
    }
    if (l + 1 == r) {
      if (Ordering[T].lt(key, bounds(l))) {
        return l - 1
      }
      else {
        return l
      }
    }
    val m = (l + r) / 2
    if (Ordering[T].gteq(key, bounds(m))) {
      binarySearch(bounds, m, r, key)
    } else {
      binarySearch(bounds, l, m, key)
    }
  }

  def binarySearch[T: Ordering: ClassTag](bounds: Array[T], key: T): Int = {
    binarySearch(bounds, 0, bounds.length, key)
  }

  def main(args: Array[String]) = {
//    val iter = Seq(Iterator(), Iterator(2, 2, 1, 4, 12, 34, 23, 2, 1, 2, 4, 1, 2, 4, 4, 5, 2, 124, 2, 124, 12), Iterator(2, 4, 2, 1, 4, 2, 4, 53, 43, 22, 2, 4, 1, 42, 242), Iterator(6, 3, 2, 4, 12, 4))
//    val res = iter.map {
//      it => {
//        val (a, b) = reservoirSampleAndCount(it, 1000)
//        (0, a, b)
//      }
//    }
//    res.foreach { r =>
//      r._2.foreach(a => print(a + " "))
//      println()
//    }
//    val bounds = mapSamplingResultToBounds(6, res.toArray)
//    bounds.foreach(a => print(a + " "))
//  }

    val arr = Array[Int](1, 4, 6, 9, 20)
    for (i <- -1 until 25) {
      println(i + " ++++++++++++++ " + binarySearch(arr, i))
    }
  }
}
