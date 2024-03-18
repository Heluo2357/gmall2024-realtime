import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object test03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.executeSql(
      """
        |create table topic_db(
        |	`database` String,
        |	`table` String,
        |	`ts` bigint,
        |	`data` map<STRING,STRING>
        |) WITH (
        |	'connector' = 'kafka',
        |	'topic' = 'topic_db',
        |	'properties.bootstrap.servers' = 'machine1:9092',
        |	'properties.group.id' = 'test03',
        |	'scan.startup.mode' = 'earliest-offset',
        |	'format' = 'json'
        |)
        |""".stripMargin)

    /*val table = tableEnv.sqlQuery(
      """
        |select *
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'comment_info'
        |""".stripMargin)*/

    tableEnv.executeSql(
      """
        |select *
        |from topic_db
        |where `database` = 'gmall'
        |and `table` = 'comment_info'
        |""".stripMargin)
      .print()

    /*tableEnv.executeSql(
      """
        |CREATE TABLE base_dic (
        |  dic_code String,
        |  dic_name STRING,
        |  parent_code String,
        |  PRIMARY KEY (dic_code) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://machine1:3306/gmall?useSSL=false&allowPublicKeyRetrieval=true',
        |   'table-name' = 'base_dic',
        |   'username' = 'root',
        |   'password' = 'jiang2357'
        |)
        |""".stripMargin)

      tableEnv.executeSql(
        """
          |select *
          |from base_dic
          |""".stripMargin)
        .print()*/

  }
}
