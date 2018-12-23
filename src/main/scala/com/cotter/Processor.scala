package com.cotter

object Processor {
  def process(histo: Map[Int, Int])(intValue: Int): Map[Int, Int] = {
    val newVal: Int = histo.getOrElse(intValue, 0) + 1
    histo + (intValue -> newVal)
  }
}

