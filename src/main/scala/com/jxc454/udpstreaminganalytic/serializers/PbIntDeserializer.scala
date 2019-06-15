package com.jxc454.udpstreaminganalytic.serializers

import java.util

import com.jxc454.models.SimpleMessages.SimpleInt
import org.apache.kafka.common.serialization.Deserializer

class PbIntDeserializer extends Deserializer[SimpleInt] {
  override def deserialize(topic: String, bytes: Array[Byte]): SimpleInt = SimpleInt.parseFrom(bytes)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}
