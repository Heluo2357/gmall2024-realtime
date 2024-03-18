package com.heluo.gmall.realtime.common.util

import com.heluo.gmall.realtime.common.constant.Constant._
import org.apache.commons.beanutils.BeanUtils
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat

import java.sql._
import scala.collection.mutable.ListBuffer

object JdbcUtil {
  def getMysqlConnection: Connection = {
    // 获取 jdbc 连接
    // 1. 加载驱动
    Class.forName(MYSQL_DRIVER);
    DriverManager.getConnection(MYSQL_URL, MYSQL_USER_NAME, MYSQL_PASSWORD);
  }

  /**
   * 执行一个查询语句, 把查询到的结果封装 T 类型的对象中.
   * @param conn mysql 连接
   * @param querySql 查询的 sql 语句: 必须是查询语句
   * @param tClass T 类
   * @param isUnderlineToCamel
   * @tparam T 每行封装的类型
   * @return 查询到的多行结果
   */
  def queryList[T](conn: Connection, querySql: String, tClass: Class[T], isUnderlineToCamel: Boolean*): List[T] = {
    var defaultIsUToC = false // 默认不执行下划线转驼峰
    if (isUnderlineToCamel.nonEmpty) defaultIsUToC = isUnderlineToCamel(0)

    val result = new ListBuffer[T]
    // 1. 预编译
    val preparedStatement: PreparedStatement = conn.prepareStatement(querySql)
    // 2. 执行查询, 获得结果集
    val resultSet: ResultSet = preparedStatement.executeQuery()
    val metaData: ResultSetMetaData = resultSet.getMetaData
    // 3. 解析结果集, 把数据封装到一个 List 集合中
    while (resultSet.next()) {
      // 变量到一行数据, 把这个行数据封装到一个 T 类型的对象中
      val t: T = tClass.newInstance()  // 使用反射创建一个 T 类型的对象
      // 遍历这一行的每一列数据
      for (i <- 1 to metaData.getColumnCount) {
        // 获取列名
        // 获取列值
        var name: String = metaData.getColumnLabel(i)
        val value: Object = resultSet.getObject(name)
        // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
        if (defaultIsUToC) {
          name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name)
        }
        // t.name=value
        BeanUtils.setProperty(t, name, value)
      }
      result += t
    }
    result.toList
  }

  @throws[SQLException]
  def closeConnection(conn: Connection): Unit = if (conn != null && !conn.isClosed) conn.close()


}
