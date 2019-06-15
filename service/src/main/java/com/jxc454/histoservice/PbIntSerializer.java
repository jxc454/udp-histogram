package com.jxc454.histoservice;

import com.jxc454.models.SimpleMessages.SimpleInt;
import org.apache.kafka.common.serialization.Serializer;

class PbIntSerializer implements Serializer<SimpleInt> {
  public byte[] serialize(String topic, SimpleInt pb) {return pb.toByteArray();}

  public void configure(java.util.Map<String, ?> configs, boolean isKey) {}

  public void close() {}
}
