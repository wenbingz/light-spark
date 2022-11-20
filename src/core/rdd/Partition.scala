package org.lighspark
package core.rdd

trait Partition extends Serializable {
  val index: Int

  override def hashCode(): Int = index

  override def equals(obj: Any): Boolean = super.equals(obj)
}
