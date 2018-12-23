package com.cotter

import java.util.Properties

import com.cotter.io.models.SimpleMessages.SimpleIntMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class ProducerCreator {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.cotter.serializers.PbIntMapSerializer")

  val producer = new KafkaProducer[String, SimpleIntMap](props)

  def produce(key: String, pb: SimpleIntMap): Unit = {
    val data = new ProducerRecord("histogram", key, pb)
    producer.send(data)
  }
}
