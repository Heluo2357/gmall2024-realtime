package com.heluo.gmall.realtime.common.util

import com.heluo.gmall.realtime.common.constant.Constant
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaRecordSerializationSchemaBuilder, KafkaSink}

import scala.util.Random

object FlinkSinkUtil {
  def getKafkaSink(topic: String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setBootstrapServers(Constant.KAFKA_BROKERS)
      .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder[String]
        .setTopic(topic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setTransactionalIdPrefix("heluo" + topic + new Random().nextLong())
      .setProperty("transaction.timeout.ms", (15 * 60 * 1000) + "")
      .build()
  }
}
