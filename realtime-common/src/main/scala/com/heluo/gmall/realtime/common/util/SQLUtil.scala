package com.heluo.gmall.realtime.common.util

import com.heluo.gmall.realtime.common.constant.Constant

object SQLUtil {
  private def getKafkaSourceSQL(topic: String, groupId: String):String={
    s"""
      |WITH (
      |	'connector' = 'kafka',
      |	'topic' = '$topic',
      |	'properties.bootstrap.servers' = '${Constant.KAFKA_BROKERS}',
      |	'properties.group.id' = '$groupId',
      |	'scan.startup.mode' = 'earliest-offset',
      |	'format' = 'json'
      |)
      |""".stripMargin
  }

  def getKafkaTopicDb(groupId: String): String = {
    s"""
      |create table topic_db(
      |	`database` String,
      |	`table` String,
      |	`ts` bigint,
      |	`data` map<STRING,STRING>
      | proc_time as PROCTIME()
      |) ${getKafkaSourceSQL(Constant.TOPIC_DB, groupId)}
      |""".stripMargin
  }

}
