package com.heluo.gmall.realtime.dim.function

import com.alibaba.fastjson.JSONObject
import com.heluo.gmall.realtime.common.bean.TableProcessDim
import com.heluo.gmall.realtime.common.constant.Constant
import com.heluo.gmall.realtime.common.util.HBaseUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.Connection

class DimHBaseSinkFunction extends RichSinkFunction[(JSONObject, TableProcessDim)]{

  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    conn = HBaseUtil.getConnection
  }

  override def invoke(value: (JSONObject, TableProcessDim), context: SinkFunction.Context): Unit = {
    val jsonObject = value._1
    val dim = value._2
    val columnType = jsonObject.getString("type")
    val data = jsonObject.getJSONObject("data")
    if ("delete".equals(columnType)){
      // 删除对应的维度表数据
      delete(data, dim)
    } else {
      // 覆盖写入维度表数据
      put(data, dim)
    }
  }

  def put(data: JSONObject, dim: TableProcessDim): Unit = {
    val sinkTable = dim.getSinkTable()
    val sinkRowKeyName = dim.getSinkRowKey()
    val sinkRowKeyValue = data.getString(sinkRowKeyName)
    val sinkFamily = dim.getSinkFamily
    HBaseUtil.putCells(conn, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data)
  }

  def delete(data: JSONObject, dim: TableProcessDim): Unit = {
    val sinkTable = dim.getSinkTable
    val sinkRowKeyName = dim.getSinkRowKey
    val sinkRowKeyValue = data.getString(sinkRowKeyName)
    HBaseUtil.deleteCells(conn, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue)
  }

  override def close(): Unit = {
    HBaseUtil.colseConnection(conn)
  }
}
