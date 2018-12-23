package com.cotter

import com.cotter.io.models.SimpleMessages.SimpleIntMap
import scala.collection.JavaConverters._

object App {

  // FIXME where do we keep the state of the histogram???

  def main(args : Array[String]): Unit = ConsumerCreator.run(intToProtobuf, Processor.process(Map[Int, Int]() /* <---STATE GOES HERE */), new ProducerCreator)

  def intToProtobuf(frequencies: Map[Int, Int]): SimpleIntMap =
    SimpleIntMap.newBuilder().putAllFrequencies(
      frequencies.map{ case (k, v) => int2Integer(k) -> int2Integer(v) }.asJava).build
}
