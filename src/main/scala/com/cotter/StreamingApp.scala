package com.cotter

import com.cotter.io.models.SimpleMessages.SimpleInt
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._


object StreamingApp {
  def main(args: Array[String]): Unit = {
//    broker.id=0, localhost:9092 ?
    val brokers: List[Int] = List(0)
    val topics: String = "protobuf"

    val conf: SparkConf = new SparkConf()
      .setAppName("StreamingApp")
      .setMaster("local[2]")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

    val topicsSet = topics.split(",").toSet

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[PbIntDeserializer])

    val messages: DStream[ConsumerRecord[String, SimpleInt]] = KafkaUtils.createDirectStream[String, SimpleInt](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, SimpleInt](topicsSet, kafkaParams))

    println("In StreamingApp")

    messages.map(k => k.value()).foreachRDD(rdd => println(rdd))
  }
}
