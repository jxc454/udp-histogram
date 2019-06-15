package com.jxc454.udpstreaminganalytic

import java.util.Properties

import com.jxc454.models.SimpleMessages.{SimpleInt, SimpleIntMap}
import com.jxc454.udpstreaminganalytic.serializers.{PbIntDeserializer, PbIntMapSerializer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._

object App extends Logging {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  def main(args : Array[String]): Unit = {
    logger.info("udpactor starting...")
    val config: Config = ConfigFactory.parseResources("streams.conf")
    logger.info(config)

    // KStream uses implicit serializers
    implicit val pbSimpleIntSerde: Serde[SimpleInt] = Serdes.serdeFrom(null, new PbIntDeserializer)
    implicit val pbSimpleIntMapSerde: Serde[SimpleIntMap] = Serdes.serdeFrom(new PbIntMapSerializer, null)

    // KStream config
    val settings: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("applicationId"))
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("bootstrapServers"))
      p
    }

    // define output table
    val builder: StreamsBuilder = new StreamsBuilder()
    val pbTable: KTable[SimpleInt, SimpleInt] = builder.stream[SimpleInt, SimpleInt](config.getString("inputTopic"))
      .groupBy((_, v) => v)
      .count
      .mapValues(i => SimpleInt.newBuilder().setIntValue(i.toInt).build)

    // define output KStream destination
    pbTable.toStream.to(config.getString("outputTopic"))

    // start
    val streams: KafkaStreams = new KafkaStreams(builder.build(), settings)
    streams.start()

    // stop
    sys.ShutdownHookThread {
      streams.close()
    }

   //    ConsumerCreator.run(intToProtobuf, Processor.process, new JsonProducerCreator)
  }

  def intToProtobuf(frequencies: Map[Int, Int]): SimpleIntMap =
    SimpleIntMap.newBuilder().putAllFrequencies(
      frequencies.map{ case (k, v) => int2Integer(k) -> int2Integer(v) }.asJava).build
}
