package com.github.jxc454.udphistogram

import java.time.Duration
import java.util.Properties

import com.github.jxc454.models.SimpleMessages.SimpleInt
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.logging.log4j.scala.Logging

import scala.collection.JavaConverters._

object Consumer extends Logging {
  def run(action: Int => Unit): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "streaming-analytic")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.cotter.PbIntDeserializer")

    val kafkaConsumer: KafkaConsumer[String, SimpleInt] = new KafkaConsumer(props)
    kafkaConsumer.subscribe(Seq("protobuf").asJava)

    while (true) {
      val records: ConsumerRecords[String, SimpleInt] = kafkaConsumer.poll(Duration.ofSeconds(1))

      val recordsSeq: Seq[ConsumerRecord[String, SimpleInt]] = records.iterator().asScala.toSeq

      recordsSeq.foreach(r => {
        logger.info("receiving...")
        logger.debug("received " + r)

        action(r.value().getIntValue)
      })
    }
  }
}
