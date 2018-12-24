package com.cotter

import java.time.Duration
import java.util.Properties

import com.cotter.io.models.SimpleMessages.{SimpleInt, SimpleIntMap}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._

object ConsumerCreator {
  def run(converter: Map[Int, Int] => SimpleIntMap, processor: (Int, Map[Int, Int]) => Map[Int, Int], producer: ProducerCreator): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "streaming-analytic")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.cotter.PbIntDeserializer")

    val kafkaConsumer: KafkaConsumer[String, SimpleInt] = new KafkaConsumer(props)
    kafkaConsumer.subscribe(Seq("protobuf").asJava)

    var state: Map[Int, Int] = Map[Int, Int]()

    while (true) {
      val records: ConsumerRecords[String, SimpleInt] = kafkaConsumer.poll(Duration.ofSeconds(1))

      val recordsSeq: Seq[ConsumerRecord[String, SimpleInt]] = records.iterator().asScala.toSeq

      recordsSeq.foreach(r => {
        println("processing")
        state = processor(r.value().getIntValue, state)

        println("producing...")
        producer.produce("", converter(state))
      })
    }
  }
}
