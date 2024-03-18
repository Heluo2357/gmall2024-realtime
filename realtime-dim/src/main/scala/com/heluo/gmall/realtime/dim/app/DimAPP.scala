package com.heluo.gmall.realtime.dim.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.heluo.gmall.realtime.common.base.BaseAPP
import com.heluo.gmall.realtime.common.bean.TableProcessDim
import com.heluo.gmall.realtime.common.constant.Constant
import com.heluo.gmall.realtime.common.util.{FlinkSourceUtil, HBaseUtil}
import com.heluo.gmall.realtime.dim.function.{DimBroadcastFunction, DimHBaseSinkFunction}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Connection

/**
 *
 */
object DimAPP extends BaseAPP {
  def main(args: Array[String]): Unit = {
    DimAPP.start(10001, 4, "dim_app", Constant.TOPIC_DB)
  }


  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    // 核心业务逻辑， 对数据进行处理
    // 1. 对ods层读取的原始数据进行数据清洗
    val jsonObjStream = etl(stream)

    // 2. 使用flinkCDC读取监控配置表数据
    val mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_Name)

    val mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource").setParallelism(1)

    // 3. 在hbase创建 维度表
    val createTableStream: DataStream[TableProcessDim] = createHBaseTable(mysqlSource).setParallelism(1)

    // 4. 做成广播流
    // 广播状态的key用于判断是否是维度表， value用于补充信息写出到hbase
    val broadcastState = new MapStateDescriptor[String, TableProcessDim]("broadcast_state", classOf[String], classOf[TableProcessDim])

    val broadcastStateStream = createTableStream.broadcast(broadcastState)

    // 5. 连接主流和广播流
    val dimStream = connectionStream(jsonObjStream, broadcastState, broadcastStateStream)

    //      dimStream.print()
    // 6. 筛选出需要写出的字段
    val filterColumnStream: DataStream[(JSONObject, TableProcessDim)] = filterColumn(dimStream)

    filterColumnStream.print()

    // 7. 写出到hbase
    filterColumnStream.addSink(new DimHBaseSinkFunction())

  }

  private def filterColumn(dimStream: DataStream[(JSONObject, TableProcessDim)]): DataStream[(JSONObject, TableProcessDim)] = {
    dimStream.map(new MapFunction[(JSONObject, TableProcessDim), (JSONObject, TableProcessDim)]() {
      override def map(value: (JSONObject, TableProcessDim)): (JSONObject, TableProcessDim) = {
        val jsonObj = value._1
        val dim = value._2

        val sinkColumns = dim.getSinkColumns
        val columns = sinkColumns.split(",")
        val data = jsonObj.getJSONObject("data")
        data.keySet().removeIf(e => !columns.contains(e))
        value
      }
    })
  }

  def connectionStream(jsonObjStream: DataStream[JSONObject], broadcastState: MapStateDescriptor[String, TableProcessDim], broadcastStateStream: BroadcastStream[TableProcessDim]): DataStream[(JSONObject, TableProcessDim)] = {
    val connectStream: BroadcastConnectedStream[JSONObject, TableProcessDim] = jsonObjStream.connect(broadcastStateStream)
    connectStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1)
  }

  def createHBaseTable(mysqlSource: DataStream[String]): DataStream[TableProcessDim] = {
    mysqlSource.flatMap(new RichFlatMapFunction[String, TableProcessDim] {
      var conn: Connection = _

      override def open(parameters: Configuration): Unit = {
        // 获取连接
        conn = HBaseUtil.getConnection
      }

      override def flatMap(t: String, collector: Collector[TableProcessDim]): Unit = {
        // 使用读取的配置表数据 到hbase中创建与之对应的表格
        val jsonObject = JSON.parseObject(t)
        val op = jsonObject.getString("op")
        val dim: TableProcessDim = new TableProcessDim()
        if (op.equals("d")) {
          val tmpJson = jsonObject.getJSONObject("before")
          dim.setSourceTable(tmpJson.getString("source_table"))
          dim.setSinkTable(tmpJson.getString("sink_table"))
          dim.setSinkFamily(tmpJson.getString("sink_family"))
          dim.setSinkColumns(tmpJson.getString("sink_columns"))
          dim.setSinkRowKey(tmpJson.getString("sink_row_key"))
          // 当配置表发送一个D类型的数据 对应HBase需要删除一张维度表
          deleteTable(dim)

        } else if (op.equals("c") || op.equals("r")) {
          val tmpJson = jsonObject.getJSONObject("after")
          println(t)
          println(tmpJson)
          dim.setSourceTable(tmpJson.getString("source_table"))
          dim.setSinkTable(tmpJson.getString("sink_table"))
          dim.setSinkFamily(tmpJson.getString("sink_family"))
          dim.setSinkColumns(tmpJson.getString("sink_columns"))
          dim.setSinkRowKey(tmpJson.getString("sink_row_key"))
          println(dim.toString)
          createTable(dim)

        } else {
          val tmpJson = jsonObject.getJSONObject("after")
          dim.setSourceTable(tmpJson.getString("source_table"))
          dim.setSinkTable(tmpJson.getString("sink_table"))
          dim.setSinkFamily(tmpJson.getString("sink_family"))
          dim.setSinkColumns(tmpJson.getString("sink_columns"))
          dim.setSinkRowKey(tmpJson.getString("sink_row_key"))
          deleteTable(dim)
          createTable(dim)

        }
        dim.setOp(op)
        collector.collect(dim)
      }

      def createTable(dim: TableProcessDim): Unit = {
        val split = dim.getSinkFamily.split(",")
        HBaseUtil.createTable(conn, Constant.HBASE_NAMESPACE, dim.getSinkTable, split: _*)
      }

      def deleteTable(dim: TableProcessDim): Unit = {
        HBaseUtil.dropTable(conn, Constant.HBASE_NAMESPACE, dim.getSinkTable)
      }

      override def close(): Unit = {
        // 关闭连接
        HBaseUtil.colseConnection(conn)
      }
    })
  }

  def etl(stream: DataStream[String]): DataStream[JSONObject] = {
    stream.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        val jsonObject: JSONObject = JSON.parseObject(t)
        val database = jsonObject.getString("database")
        val dataType = jsonObject.getString("type")
        val data = jsonObject.getString("data")
        if ("gmall".equals(database) &&
          !"bootstrap-complete".equals(dataType) &&
          !"bootstrap-start".equals(dataType) &&
          data != null && data.nonEmpty) {
          collector.collect(jsonObject)
        }
      }
    })
  }

}




