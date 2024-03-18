package com.heluo.gmall.realtime.dwd.db.split.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.heluo.gmall.realtime.common.base.BaseAPP
import com.heluo.gmall.realtime.common.constant.Constant
import com.heluo.gmall.realtime.common.util.{DateFormatUtil, FlinkSinkUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}

import java.time.Duration

object DwdBaseLog extends BaseAPP {
  def main(args: Array[String]): Unit = {
    DwdBaseLog.start(10011, 4, "DwdBaseLog", Constant.TOPIC_LOG)
  }

  override def handle(env: StreamExecutionEnvironment, stream: DataStream[String]): Unit = {
    // 核心业务处理
    // 1. 进行ETL过滤不完整的数据
    //    stream.print()
    val jsonObjectStream = elt(stream)

    // 2. 进行新旧访客修复
    val keyedStream: KeyedStream[JSONObject, String] = keyByWithWaterMark(jsonObjectStream)

    val isNewFixStream = isNewFix(keyedStream)

    //    isNewFixStream.print()
    // 3. 拆分不同类型的用户行为日志
    // 启动日志: 启动信息 报错信息
    // 页面日志: 页面信息 曝光信息 动作信息 报错信息
    val startTag = OutputTag[String]("start")
    val errorTag: OutputTag[String] = OutputTag[String]("err")
    val displayTag = OutputTag[String]("display")
    val actionTag = OutputTag[String]("action")
    val pageStream = splitLog(isNewFixStream, startTag, errorTag, displayTag, actionTag)

    val startStream = pageStream.getSideOutput(startTag)
    val errorStream = pageStream.getSideOutput(errorTag)
    val displayStream = pageStream.getSideOutput(displayTag)
    val actionStream = pageStream.getSideOutput(actionTag)


    pageStream.print("page")
    startStream.print("start")
    errorStream.print("err")
    displayStream.print("display")
    actionStream.print("action")

    pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE))
    startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START))
    errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR))
    displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY))
    actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION))

  }


  def splitLog(isNewFixStream: DataStream[JSONObject], startTag: OutputTag[String], errorTag: OutputTag[String], displayTag: OutputTag[String], actionTag: OutputTag[String]): DataStream[String] = {
    isNewFixStream.process(new ProcessFunction[JSONObject, String] {
      override def processElement(value: JSONObject, ctx: ProcessFunction[JSONObject, String]#Context, out: Collector[String]): Unit = {
        // 核心逻辑 根据输出的不同 拆分到不同的侧输出流
        val err = value.getJSONObject("err")
        if (err != null) {
          // 当前存在报错信息
          println(err.toJSONString)
          ctx.output(errorTag, err.toJSONString)
          value.remove("err")
        }

        val page = value.getJSONObject("page")
        val start = value.getJSONObject("start")
        val common = value.getJSONObject("common")
        val ts = value.getLong("ts")
        if (start != null) {
          // 当前是启动日志
          // 注意 输出的是value完整的日志信息
          ctx.output(startTag, value.toJSONString)
        } else if (page != null) {
          // 当前为页面日志
          val displays = value.getJSONArray("displays")
          if (displays != null) {
            for (i <- 0 until displays.size()) {
              val display = displays.getJSONObject(i)
              display.put("common", common)
              display.put("ts", ts)
              display.put("page", page)
              ctx.output(displayTag, display.toJSONString)
            }
            value.remove("displays")
          }


          val actions = value.getJSONArray("actions")
          if (actions != null) {
            for (i <- 0 until actions.size()) {
              val action = actions.getJSONObject(i)
              action.put("common", common)
              action.put("ts", ts)
              action.put("page", page)
              ctx.output(actionTag, action.toJSONString)
            }
            value.remove("actions")
          }

          // 只保留page信息 写出到主流
          out.collect(value.toJSONString)
        } else {
          // 留空
        }

      }
    })
  }

  def isNewFix(keyedStream: KeyedStream[JSONObject, String]): DataStream[JSONObject] = {
    keyedStream.process(new KeyedProcessFunction[String, JSONObject, JSONObject] {
      // 创建状态 此状态是为了保存每个用户的记录（例如：如果一个新用户来到数仓，则is_new为1，
      // 并且进入时间与系统时间是同一天则的确是一个新用户。不是老用户很久没有访问到。
      // 介于采集到的数据也可能是很久之前的业务用户数据，则每个用户也都有is_new=1的情况，
      // 但是有可能没有采集到之后时间线的数据。所以就把这个视为老用户伪装新用户。也就是is_new=1的数据先比is_new=0来）
      lazy val firstLoginDtState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("first_login_dt", classOf[String]))

      override def processElement(value: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
        // 1. 获取当前数据的is_new字段
        val common = value.getJSONObject("common")
        val isNew = common.getString("is_new")
        val firstLoginDt = firstLoginDtState.value()
        val ts = value.getLong("ts")
        val curDt = DateFormatUtil.tsToDate(ts)
        if (isNew.equals("1")) {
          // 判断当前状态情况 firstLoginDt != null说明之前有对此用户相关数据到来 所以将其is_new更改为0
          if (firstLoginDt != null && firstLoginDt.equals(curDt)) {
            // 如果状态不为空 日期他不是今天 说明当前数据错误 不是新访客 伪装新访客
            common.put("is_new", 0)
            // firstLoginDt == null 则说明的确是新用户 状态没有记录到之前的对于该用户的日志数据
          } else if (firstLoginDt == null) {
            // 状态为空
            firstLoginDtState.update(curDt)
          } else {
            // 留空
            // 当前数据是同一天新访客重复登录
          }
        } else if (isNew.equals("0")) {
          // is_new 为 0 说明是老用户 firstLoginDt == null则说明之前没有对该用户记录
          if (firstLoginDt == null) {
            // 老用户 flink实时数仓里面还有没有记录过这个访客 需要补充访客信息
            // 把访客首次登录日期补充一个值 今天以前的任意一天任务都可以 使用昨天的日期
            firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L))

          } else {
            // 流空
            // 正常情况 不需要修复

          }

        } else {
          // 当前 数据is_new 不为0， 也不为1 是错位数据
        }
        collector.collect(value)
      }
    })
  }

  def keyByWithWaterMark(jsonStream: DataStream[JSONObject]): KeyedStream[JSONObject, String] = {
    jsonStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3L))
      .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = t.getLong("ts")
      }))
      .keyBy(_.getJSONObject("common").getString("mid"))
  }

  def elt(dataStream: DataStream[String]): DataStream[JSONObject] = {
    dataStream.flatMap(new FlatMapFunction[String, JSONObject] {
      override def flatMap(t: String, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject = JSON.parseObject(t)
          val page = jsonObject.getJSONObject("page")
          val start = jsonObject.getJSONObject("start")
          val common = jsonObject.getJSONObject("common")
          val ts = jsonObject.getLong("ts")
          if (page != null || start != null) {
            if (common != null && common.getString("mid") != null && ts != null) collector.collect(jsonObject)
          }
        } catch {
          case e: Exception => {
            //            e.printStackTrace()
            println("过滤掉脏数据" + t)
          }
        }
      }
    })
  }
}
