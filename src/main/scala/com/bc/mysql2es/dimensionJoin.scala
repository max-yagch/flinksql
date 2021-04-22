package com.bc.mysql2es

import java.io.InputStream
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableSchema, Types}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import org.apache.flink.connector.jdbc.internal.options.{JdbcLookupOptions, JdbcOptions}
import org.apache.flink.connector.jdbc.table.JdbcTableSource


object dimensionJoin {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //  1、创建blink版本的流式查询环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    //设置故障恢复策略：任务失败的时候自动每隔10秒重启，一共尝试重启3次



    //读取配置文件
    import org.apache.flink.api.java.utils.ParameterTool
    //配置文件放在resource目录
    val stream: InputStream = this.getClass.getClassLoader.getResourceAsStream("config.properties")
    val parameter: ParameterTool = ParameterTool.fromPropertiesFile(stream)
    //设置为全局变量
    env.getConfig.setGlobalJobParameters(parameter)
    val hadoopUserName: String = parameter.get("HADOOP_USER_NAME")
    System.setProperty("HADOOP_USER_NAME", hadoopUserName)
    val enableCheckpointing: Long = parameter.get("enableCheckpointing").toLong
    val checkpointPausetime: Long = parameter.get("checkpointPausetime").toLong
    val checkpointTimeTimeout: Long = parameter.get("checkpointTimeTimeout").toLong
    val MaxConcurrentCheckpoints: Int = parameter.get("MaxConcurrentCheckpoints").toInt
    val AutoWatermarkInterval: Long = parameter.get("AutoWatermarkInterval").toLong


    //设置checkpoint
    env.enableCheckpointing(enableCheckpointing)
    val config = env.getCheckpointConfig

    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 设置CheckPoint模式为EXACTLY_ONCE，这个对于kafka 端到端一致性极其重要
    // 取消任务时保留CheckPoint
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //启动时从CheckPoint恢复任务
    //config.setPreferCheckpointForRecovery(true)
    config.setMinPauseBetweenCheckpoints(checkpointPausetime) //设置checkpoint间隔时间20分钟
    config.setCheckpointTimeout(checkpointTimeTimeout) //设置checkpoint超时时间
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(MaxConcurrentCheckpoints)

    //设置状态后端 //设置checkpoint的保存目录
    //val rocksDBStateBackend: String = parameter.get("rocksDBStateBackend")

    //val stateBackend: StateBackend = new RocksDBStateBackend(rocksDBStateBackend)
    //env.setStateBackend(stateBackend)

    //设置水印和时间 使用事件时间
    env.getConfig.setAutoWatermarkInterval(AutoWatermarkInterval)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置故障恢复策略：任务失败的时候自动每隔10秒重启，一共尝试重启3次
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      5, // 尝试重启的次数
      org.apache.flink.api.common.time.Time.of(100, TimeUnit.SECONDS) // 延时
    ))









    //默认并行度
    env.setParallelism(3)



    //设置topic
    val data_version_topic: String = "ods_spider_maxwell1_data_version_full_add"
    val data_version_topic_List: List[String] = data_version_topic.split(",").map(_.trim).toList
    val sales_order_detail_topic: String = "ods_spider_maxwell1_sales_order_detail_full_add"
    val sales_order_detail_topic_List: List[String] = sales_order_detail_topic.split(",").map(_.trim).toList
    import scala.collection.JavaConverters._
    val data_version_topics: util.List[String] = data_version_topic_List.toBuffer.asJava
    val sales_order_detail_topics: util.List[String] = sales_order_detail_topic_List.toBuffer.asJava
    val prop = new Properties()
    //kafka source

    val bootstrapServer: String = parameter.get("bootstrapservers")
    val groupId: String = parameter.get("groupid")
    prop.setProperty("bootstrap.servers", bootstrapServer)
    prop.setProperty("group.id", groupId)

    val watermarkDelay: Long = parameter.get("watermarkDelay").toLong
    //读取把kafka中的json 数据读取为json格式 使用jeckson
    val data_versionSource: FlinkKafkaConsumer011[ObjectNode] = new FlinkKafkaConsumer011[ObjectNode](
      data_version_topics,
      new JSONKeyValueDeserializationSchema(true),
      prop)


    val sales_order_detailSource: FlinkKafkaConsumer011[ObjectNode] = new FlinkKafkaConsumer011[ObjectNode](
      sales_order_detail_topics,
      new JSONKeyValueDeserializationSchema(true),
      prop)



    val sales_order_detailDStream: DataStream[ObjectNode] = env.addSource(sales_order_detailSource).setParallelism(1)


    val data_versionDstream: DataStream[ObjectNode] = env.addSource(data_versionSource).setParallelism(1)



    val SalesOrderDetailDStream: DataStream[SalesOrderDetail] = sales_order_detailDStream.map(new salesorderdetailmap)

    val DataVersionDStream: DataStream[DataVersion] = data_versionDstream.map(new dataversionmap)

    //SalesOrderDetailDStream.print("SalesOrderDetailDStream")
    //DataVersionDStream.print("DataVersionDStream")


    val SalesOrderDetailTable: Table = streamTableEnv.fromDataStream(SalesOrderDetailDStream)
    streamTableEnv.registerTable("SalesOrderDetailTable",SalesOrderDetailTable)



    //5、temporal function join
   /* val mysqlFieldNames: Array[String] = Array("id", "data_string", "create_time", "update_time")
    val mysqlFieldTypes: Array[DataType] = Array(DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.TIMESTAMP(), DataTypes.TIMESTAMP())


    val mysqlTableSchema: TableSchema = TableSchema.builder().fields(mysqlFieldNames, mysqlFieldTypes).build()
    val options: JdbcOptions = JdbcOptions.builder()
      .setDriverName("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://rm-bp1d054150ompoqgfoo.mysql.rds.aliyuncs.com:3306/maxwell1")
      .setUsername("xier_dba")
      .setPassword("Dba9527@")
      .setTableName("data_version")
      .build()


    val lookupOptions: JdbcLookupOptions = JdbcLookupOptions.builder()
      .setCacheExpireMs(10 * 1000) //缓存有效期
      .setCacheMaxSize(10) //最大缓存数据条数
      .setMaxRetryTimes(3) //最大重试次数
      .build()

    val jdbcTableSource: JdbcTableSource = JdbcTableSource.builder()
      .setOptions(options)
      .setLookupOptions(lookupOptions)
      .setSchema(mysqlTableSchema)
      .build()
    streamTableEnv.registerTableSource("mysql",jdbcTableSource);
    //注册TableFunction
    streamTableEnv.registerFunction("mysqlLookup",jdbcTableSource.getLookupFunction(Array("id")));







    /**
     * 仅支持Blink planner
     * 仅支持SQL，目前不支持Table API
     * 目前不支持基于事件时间(event time)的temporal table join
     * 维表可能会不断变化，JOIN行为发生后，维表中的数据发生了变化（新增、更新或删除），则已关联的维表数据不会被同步变化
     * 维表和维表不能进行JOIN
     * 维表必须指定主键。维表JOIN时，ON的条件必须包含所有主键的等值条件
     */


   val table: Table = streamTableEnv.sqlQuery(
      """
        |select
        |
        |
        |*
        |from SalesOrderDetailTable t1,
        |LATERAL TABLE (mysqlLookup(t1.data_version)) as t2
        |where  t1.data_version = t2.id
        |""".stripMargin)

    streamTableEnv.toAppendStream[Row](table).print()*/


    //temporal join
    streamTableEnv.sqlQuery(
      """
        |CREATE TABLE data_version (
        |id bigint ,
        | data_string varchar,
        | create_time timestamp,
        | update_time timestamp,
        |  PRIMARY KEY (id)  NOT ENFORCED
        |) WITH (
        |
        |   'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://rm-bp1d054150ompoqgfoo.mysql.rds.aliyuncs.com:3306/maxwell1',
        |    'connector.table' = 'data_version',
        |    'connector.driver' = 'com.mysql.jdbc.Driver',
        |    'connector.username' = 'xier_dba',
        |    'connector.password' = 'Dba9527@',
        |    'connector.lookup.cache.max-rows' = '5000',
        |    'connector.lookup.cache.ttl' = '10min'
        |)
        |""".stripMargin)

    val table: Table = streamTableEnv.sqlQuery(
      """
        |select
        |*
        |from SalesOrderDetailTable t1
        |join data_version t2 on t1.data_version = t2.id
        |""".stripMargin)

    streamTableEnv.toAppendStream[Row](table).print()
    env.execute()

  }

}
