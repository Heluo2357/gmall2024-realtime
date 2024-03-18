package com.heluo.gmall.realtime.common.util

import com.alibaba.fastjson.JSONObject
import com.heluo.gmall.realtime.common.constant.Constant
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, TableName}

import java.io.IOException
import scala.collection.JavaConverters.asJavaCollectionConverter


object HBaseUtil {
  def getConnection: Connection = {
    val configuration = new Configuration()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, Constant.HBASE_ZOOKEEPER_QUORUM)
    ConnectionFactory.createConnection(configuration)
  }

  /**
   * 关闭连接
   * @param connection HBase的同步连接
   */
  def colseConnection(connection: Connection): Unit = if (connection != null && !connection.isClosed)  connection.close()

  /**
   * 判断表格是否存在
   *
   * @param namespace 命名空间名称
   * @param tableName 表格名称
   * @return true表示存在，false表示不存在
   */
  def isTableExists(conn: Connection, namespace: String, tableName: String): Boolean = {
    val admin = conn.getAdmin

    val bool: Boolean = try {
      admin.tableExists(TableName.valueOf(namespace, tableName))
    } catch {
      case e: IOException =>
        e.printStackTrace()
        false
    }
    admin.close()
    bool
  }

  /**
   * 创建表格
   * @param connection HBase同步连接
   * @param namespace 命名空间
   * @param table 表名
   * @param familyColum 列族
   */
  def createTable(connection: Connection, namespace: String, table: String, familyColum: String*): Unit = {
    if (familyColum.isEmpty || familyColum == null) {
      return
    }

    if (isTableExists(connection, namespace, table)) {
      println(s"$namespace:$table 表格已经存在")
      return
    }
    // 1. 获取admin
    val admin = connection.getAdmin

    // 2. 创建表格描述
    val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table))

    familyColum.foreach(e => {
      val familyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(e)).build()
      tableDescriptorBuilder.setColumnFamily(familyDescriptorBuilder)
    })
    // 简洁代码

    /*val familyArr = familyColum.map(e => ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(e)).build()).asJavaCollection
    tableDescriptor.setColumnFamilies(familyArr)*/

    // 3. 使用admin调用方法创建表格
    admin.createTable(tableDescriptorBuilder.build())

    // 4. 关闭admin连接
    admin.close()
  }

  /**
   * 删除表格
   * @param connection HBase的同步连接
   * @param namesparce 命名空间
   * @param table 表名
   */
  def dropTable(connection: Connection, namesparce: String, table: String): Unit = {
    // 1. 获取admin
    val admin = connection.getAdmin

    // 2. 调用方法删除表格
    admin.disableTable(TableName.valueOf(namesparce, table))
    admin.deleteTable(TableName.valueOf(namesparce, table))

    // 3. 关闭admin
    admin.close()
  }

  /**
   * 写数据到HBase
   * @param connection 一个同步连接
   * @param namespace 命名空间
   * @param tableName 表名
   * @param rowKey 主键
   * @param family 列族
   * @param data 列名和列值
   */
  def putCells(connection: Connection, namespace: String, tableName: String, rowKey: String, family: String, data: JSONObject): Unit = {
    // 1. 获取table
    val table = connection.getTable(TableName.valueOf(namespace, tableName))

    // 2. 创建写入对象
    val put = new Put(Bytes.toBytes(rowKey))

    data.keySet.forEach(column => {
      val columnValue = data.getString(column)
      if (columnValue != null) put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(columnValue))
    })

    // 3. 调用方法写出数据
    try table.put(put)
    catch {case e: IOException => e.printStackTrace()}


    // 4. 关闭table
    table.close()
  }

  /**
   * 删除一整行数据
   * @param connection 一个同步连接
   * @param nameSpace 命名空间名词
   * @param tableName 表格
   * @param rowKey 主键
   */
  def deleteCells(connection: Connection, nameSpace: String, tableName: String, rowKey: String): Unit = {
    // 1. 获取table
    val table = connection.getTable(TableName.valueOf(nameSpace, tableName))

    // 2. 创建删除对象
    val delete = new Delete(Bytes.toBytes(rowKey))

    // 3. 调用方法删除数据
    try table.delete(delete)
    catch {
      case e: IOException => e.printStackTrace()
    }

    table.close()
  }
}
