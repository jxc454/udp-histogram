package com.jxc454.udpstreaminganalytic.serializers

import java.util

import com.jxc454.models.SimpleMessages.SimpleInt
import org.apache.kafka.common.serialization.Serializer

class PbIntSerializer extends Serializer[SimpleInt] {
  override def serialize(topic: String, pb: SimpleInt): Array[Byte] = pb.toByteArray

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}