package com.heluo.gmall.realtime.dim.function

import com.alibaba.fastjson.JSONObject
import com.heluo.gmall.realtime.common.bean.TableProcessDim
import com.heluo.gmall.realtime.common.util.JdbcUtil
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class DimBroadcastFunction() extends BroadcastProcessFunction[JSONObject, TableProcessDim, (JSONObject, TableProcessDim)] {
   var hashMap: mutable.HashMap[String, TableProcessDim] = _
   var broadcastState: MapStateDescriptor[String, TableProcessDim] = _

  def this(broadcastState: MapStateDescriptor[String, TableProcessDim]) = {
    this()
    this.broadcastState = broadcastState
  }

  override def open(parameters: Configuration): Unit = {
    // 预加载初始的维度表信息
    val connection = JdbcUtil.getMysqlConnection
    val tableProcessDims: List[TableProcessDim] = JdbcUtil.queryList(connection,
      "select * from gmall2024_config.table_process_dim", classOf[TableProcessDim],
      true)
    hashMap = new mutable.HashMap[String, TableProcessDim]()
    tableProcessDims.foreach(e => {
      e.setOp("r")
      hashMap.put(e.sourceTable, e)
    })
    JdbcUtil.closeConnection(connection)
  }

  /**
   * 处理主流数据
   *
   * @param value
   * @param cxt
   * @param out
   */
  override def processBroadcastElement(value: TableProcessDim, cxt: BroadcastProcessFunction[JSONObject, TableProcessDim, (JSONObject, TableProcessDim)]#Context, out: Collector[(JSONObject, TableProcessDim)]): Unit = {
    // 读取广播状态
    val tableProcessState: BroadcastState[String, TableProcessDim] = cxt.getBroadcastState(broadcastState)
    // 将配置表信息作为一个维度表的标记 写到广播状态
    val op = value.getOp
    if (op.equals("d")) {
      tableProcessState.remove(value.getSourceTable)
      // 同步删除hashmap中初始化加载的配置表信息
      hashMap.remove(value.getSourceTable)
    } else {
      tableProcessState.put(value.getSourceTable, value)
    }
  }

  /**
   * 处理广播流数据
   *
   * @param value
   * @param cxt
   * @param out
   */
  override def processElement(value: JSONObject, cxt: BroadcastProcessFunction[JSONObject, TableProcessDim, (JSONObject, TableProcessDim)]#ReadOnlyContext, out: Collector[(JSONObject, TableProcessDim)]): Unit = {
    // 读取广播状态
    val tableProcessState: ReadOnlyBroadcastState[String, TableProcessDim] = cxt.getBroadcastState(broadcastState)
    // 查询广播状态 判断当前的数据对应的表格是否存在于状态里面
    val tableName = value.getString("table")

    var tableProcessDim = tableProcessState.get(tableName)

    // 如果是数据到的太早 造成状态为空
    if (tableProcessDim == null) {
      tableProcessDim = hashMap(tableName)
    }

    if (tableProcessDim != null) {
      // 状态不为空 说明当前一行数据是维度表数据
      out.collect((value, tableProcessDim))
    }
  }
}
