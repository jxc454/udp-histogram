package com.jxc454.udpstreaminganalytic

import com.jxc454.models.SimpleMessages.SimpleIntMap
import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import scala.collection.JavaConverters._

object App {

  def main(args : Array[String]): Unit = {
//    ConsumerCreator.run(intToProtobuf, Processor.process, new JsonProducerCreator)


  }

  def intToProtobuf(frequencies: Map[Int, Int]): SimpleIntMap =
    SimpleIntMap.newBuilder().putAllFrequencies(
      frequencies.map{ case (k, v) => int2Integer(k) -> int2Integer(v) }.asJava).build
}
