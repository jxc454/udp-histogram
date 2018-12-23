package com.cotter

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import com.cotter.io.models.SimpleMessages.SimpleIntMap
import java.util.Properties
import java.time.Duration
import java.util.UUID

import scala.collection.JavaConverters._

object ConsumerCreator {
  def run(converter: Map[Int, Int] => SimpleIntMap, processor: Int => Map[Int, Int], producer: ProducerCreator): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "streaming-analytic")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.cotter.udpstreaminganalytic.PbIntDeserializer")

    val kafkaConsumer: KafkaConsumer[String, SimpleIntMap] = new KafkaConsumer(props)
    kafkaConsumer.subscribe(Seq("protobuf").asJava)

    while (true) {
      val records: ConsumerRecords[String, SimpleIntMap] = kafkaConsumer.poll(Duration.ofSeconds(1))

      val recordsSeq: Seq[ConsumerRecord[String, SimpleIntMap]] = records.iterator().asScala.toSeq

      recordsSeq.foreach(r => {
        println("processing")
        val newMap: Map[Int, Int] = processor(r.value())

        println("producing...")
        producer.produce(UUID.randomUUID().toString, )
      })
    }
  }
}
