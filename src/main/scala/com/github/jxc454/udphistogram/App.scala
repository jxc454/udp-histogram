package com.github.jxc454.udphistogram

import com.github.jxc454.models.SimpleMessages.SimpleIntMap

import scala.collection.JavaConverters._

object App {

  def main(args : Array[String]): Unit = ConsumerCreator.run(intToProtobuf, Processor.process, new JsonProducerCreator)

  def intToProtobuf(frequencies: Map[Int, Int]): SimpleIntMap =
    SimpleIntMap.newBuilder().putAllFrequencies(
      frequencies.map{ case (k, v) => int2Integer(k) -> int2Integer(v) }.asJava).build
}