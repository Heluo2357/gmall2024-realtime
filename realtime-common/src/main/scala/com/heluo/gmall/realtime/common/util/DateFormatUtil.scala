package com.heluo.gmall.realtime.common.util

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

object DateFormatUtil {
  private val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val dtfForPartition: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val dtfFull: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
   * 2023-07-05 01:01:01 转成 ms 值
   *
   * @param dateTime
   * @return
   */
  def dateTimeToTs(dateTime: String): Long = {
    val localDateTime: LocalDateTime = LocalDateTime.parse(dateTime, dtfFull)
    localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli
  }

  /**
   * 把毫秒值转成 年月日:  2023-07-05
   *
   * @param ts
   * @return
   */
  def tsToDate(ts: Long): String = {
    val dt: Date = new Date(ts)
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(dt.toInstant, ZoneId.systemDefault());
    dtf.format(localDateTime)
  }

  /**
   * 把毫秒值转成 年月日时分秒:  2023-07-05 01:01:01
   *
   * @param ts
   * @return
   */
  def tsToDateTime(ts: Long): String = {
    val dt: Date = new Date(ts)
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(dt.toInstant, ZoneId.systemDefault());
    dtfFull.format(localDateTime);
  }

  def tsToDateForPartition(ts: Long): String = {
    val dt: Date = new Date(ts)
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(dt.toInstant, ZoneId.systemDefault());
    dtfForPartition.format(localDateTime)
  }

  /**
   * 把 年月日转成 ts
   *
   * @param date
   * @return
   */
  def dateToTs(date: String): Long = dateTimeToTs(date + " 00:00:00")
}
