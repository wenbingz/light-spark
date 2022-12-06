package org.lighspark.core.rdd

class DoubleRDDFunctions(self: RDD[Double]) extends Serializable {
  def sum(): Double = {
    self.reduce((a, b) => a + b)
  }

  def min(): Double = {
    self.reduce((a, b) => scala.math.min(a, b))
  }

  def max(): Double = {
    self.reduce((a, b) => scala.math.max(a, b))
  }
}
