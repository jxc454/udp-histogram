package com.jxc454.histoservice;

import com.jxc454.models.SimpleMessages.SimpleInt;
import com.jxc454.models.shaded.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

public class PbIntDeserializer implements Deserializer<SimpleInt> {
  public SimpleInt deserialize(String topic, byte[] data){
      try {
          return SimpleInt.parseFrom(data);
      } catch (final InvalidProtocolBufferException e) {
          throw new RuntimeException("Received unparseable message " + e.getMessage(), e);
      }
  }

  public void configure(java.util.Map<String, ?> configs, boolean isKey) {}

  public void close() {}
}
