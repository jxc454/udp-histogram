package com.jxc454.udpstreaminganalytic.serializers

import java.util

import com.jxc454.models.SimpleMessages.SimpleIntMap
import org.apache.kafka.common.serialization.Deserializer

class PbIntMapDeserializer extends Deserializer[SimpleIntMap] {
  override def deserialize(topic: String, bytes: Array[Byte]): SimpleIntMap = SimpleIntMap.parseFrom(bytes)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}
