package com.heluo.gmall.realtime.common.bean

import scala.beans.BeanProperty

class TableProcessDim() {

  // 来源表格
  @BeanProperty
  var sourceTable: String = _

  // 目标表名
  @BeanProperty
  var sinkTable: String = _

  // 输出字段
  @BeanProperty
  var sinkColumns: String = _

  // 数据到hbase的列族
  @BeanProperty
  var sinkFamily: String = _

  // sink到hbase的时候的主键字段
  @BeanProperty
  var sinkRowKey: String = _

  // 配置表操作类型
  @BeanProperty
  var op: String = _

  override def toString: String = "TableProcessDim{" + sourceTable + ", " + sinkTable + ", " + sinkColumns + ", " + sinkFamily + ", " + sinkRowKey + ", " + op + "}"
}
