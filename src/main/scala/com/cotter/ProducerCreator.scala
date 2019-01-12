package com.cotter

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Properties

import com.cotter.io.models.SimpleMessages.SimpleIntMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.logging.log4j.scala.Logging
import org.json4s.NoTypeHints
import org.json4s.native.Serialization.write

import scala.collection.JavaConverters._

sealed trait ProducerCreator {
  def produce(key: String, pb: SimpleIntMap)
}

class TopicProducerCreator extends ProducerCreator with Logging {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.cotter.serializers.PbIntMapSerializer")

  val producer = new KafkaProducer[String, SimpleIntMap](props)

  def produce(key: String, pb: SimpleIntMap): Unit = {
    logger.info("producing: " + pb.toString)

    val data = new ProducerRecord("histogram", key, pb)
    producer.send(data)
  }
}

class JsonProducerCreator extends ProducerCreator with Logging {
  def produce(key: String, pb: SimpleIntMap): Unit = {
    // magic to convert Map to json string
    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints)
    val json: String = write(pb.getFrequenciesMap.asScala)

    logger.debug("writing: " + json)

    // write to tmp
    val file = new File("/tmp/udpHistogram.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(json + "\n")
    bw.close()
    println(pb.toString)
  }
}