package com.heluo.gmall.realtime.common.util

import com.heluo.gmall.realtime.common.constant.Constant
import com.nimbusds.jose.util.StandardCharset
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{Types, TypeInformation}

import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

import java.util.Properties

object FlinkSourceUtil {
  def getKafkaSource(ckAndGroupID: String, topicName: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(topicName)
      .setBootstrapServers(Constant.KAFKA_BROKERS)
      .setGroupId(ckAndGroupID)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(
        // SimpleStringSchema无法反序列化null值 会直接报错
        // 后续DWD层会向kafka发送null 不能使用SimpleStringSchema
        // new SimpleStringSchema()
        new DeserializationSchema[String] {
          override def deserialize(bytes: Array[Byte]): String = {
            if (bytes != null && bytes.length != 0) new String(bytes, StandardCharset.UTF_8) else ""
          }

          override def isEndOfStream(t: String): Boolean = false

          override def getProducedType: TypeInformation[String] = Types.STRING
        }
      )
      .build()
  }

  def getMySqlSource(database: String, table: String): MySqlSource[String] = {
    val props = new Properties();
    props.setProperty("useSSL", "false");
    props.setProperty("allowPublicKeyRetrieval", "true");

    MySqlSource.builder[String]()
      .hostname(Constant.MYSQL_HOST)
      .port(Constant.SQL_PORT)
      .username(Constant.MYSQL_USER_NAME)
      .password(Constant.MYSQL_PASSWORD)
      .jdbcProperties(props)
      .databaseList(database)
      .tableList(database + "." + table)
      .deserializer(new JsonDebeziumDeserializationSchema())
      .startupOptions(StartupOptions.initial())
      .build()
  }
}
