package com.heluo.gmall.realtime.common.constant

object Constant {
  val KAFKA_BROKERS: String = "machine1:9092,machine2:9092,machine3:9092"

  val TOPIC_DB: String = "topic_db"
  val TOPIC_LOG: String = "topic_log"

  val MYSQL_HOST: String = "machine1"
  val SQL_PORT: Int = 3306;
  val MYSQL_USER_NAME: String = "root"
  val MYSQL_PASSWORD: String = "jiang2357"
  val PROCESS_DATABASE: String = "gmall2024_config"
  val PROCESS_DIM_TABLE_Name: String = "table_process_dim"

  val HBASE_NAMESPACE: String = "gmall"
  val HBASE_ZOOKEEPER_QUORUM = "machine1,machine2,machine3"

  val MYSQL_DRIVER: String = "com.mysql.cj.jdbc.Driver"
  val MYSQL_URL: String = s"jdbc:mysql://$MYSQL_HOST:3306?useSSL=false"

  val TOPIC_DWD_TRAFFIC_START: String = "dwd_traffic_start"
  val TOPIC_DWD_TRAFFIC_ERR: String = "dwd_traffic_err"
  val TOPIC_DWD_TRAFFIC_PAGE: String = "dwd_traffic_page"
  val TOPIC_DWD_TRAFFIC_ACTION: String = "dwd_traffic_action"
  val TOPIC_DWD_TRAFFIC_DISPLAY: String = "dwd_traffic_display"

  val TOPIC_DWD_INTERACTION_COMMENT_INFO: String = "dwd_interaction_comment_info"
  val TOPIC_DWD_TRADE_CART_ADD: String = "dwd_trade_cart_add"

  val TOPIC_DWD_TRADE_ORDER_DETAIL: String = "dwd_trade_order_detail"

  val TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel"

  val TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success"
  val TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund"

  val TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success"

  val TOPIC_DWD_USER_REGISTER = "dwd_user_register"

}

