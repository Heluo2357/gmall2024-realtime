import com.heluo.gmall.realtime.common.constant.Constant
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Test02 {
  def main(args: Array[String]): Unit = {
    // 1. 构建flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    // 2.添加检查点和状态后端的参数
    // 1.4 状态后端及检查点相关配置
    // 1.4.1 设置状态后端
   /* env.setStateBackend(new HashMapStateBackend())

    // 1.4.2 开启 checkpoint
    env.enableCheckpointing(5000)
    // 1.4.3 设置 checkpoint 模式: 精准一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 1.4.4 checkpoint 存储
    env.getCheckpointConfig.setCheckpointStorage("hdfs://machine1:8020/gmall2024/stream/" + "test01")
    // 1.4.5 checkpoint 并发数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 1.4.6 checkpoint 之间的最小间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    // 1.4.7 checkpoint  的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    // 1.4.8 job 取消时 checkpoint 保留策略
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION)*/

    // 3.读取数据
    val mysqlSource = MySqlSource.builder[String]()
      .hostname(Constant.MYSQL_HOST)
      .port(Constant.SQL_PORT)
      .username(Constant.MYSQL_USER_NAME)
      .password(Constant.MYSQL_PASSWORD)
      .databaseList("gmall2024_config")
      .tableList("gmall2024_config.table_process_dim")
      .deserializer(new JsonDebeziumDeserializationSchema())
      .startupOptions(StartupOptions.initial())
      .build()

    val kafkaSource = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource").setParallelism(1)

    // 4.对数据进行处理
    kafkaSource.print()

    env.execute()
  }
}
