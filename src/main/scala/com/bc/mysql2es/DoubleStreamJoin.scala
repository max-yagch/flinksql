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
import org.apache.flink.table.api.{EnvironmentSettings, Table, Types}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row


object DoubleStreamJoin {
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
    val sales_order_topic: String = "ods_spider_maxwell1_sales_order_full_add"
    val sales_order_topic_List: List[String] = sales_order_topic.split(",").map(_.trim).toList
    val sales_order_detail_topic: String = "ods_spider_maxwell1_sales_order_detail_full_add"
    val sales_order_detail_topic_List: List[String] = sales_order_detail_topic.split(",").map(_.trim).toList
    import scala.collection.JavaConverters._
    val sales_order_topics: util.List[String] = sales_order_topic_List.toBuffer.asJava
    val sales_order_detail_topics: util.List[String] = sales_order_detail_topic_List.toBuffer.asJava
    val prop = new Properties()
    //kafka source

    val bootstrapServer: String = parameter.get("bootstrapservers")
    val groupId: String = parameter.get("groupid")
    prop.setProperty("bootstrap.servers", bootstrapServer)
    prop.setProperty("group.id", groupId)

    val watermarkDelay: Long = parameter.get("watermarkDelay").toLong
    //读取把kafka中的json 数据读取为json格式 使用jeckson
    val sales_orderSource: FlinkKafkaConsumer011[ObjectNode] = new FlinkKafkaConsumer011[ObjectNode](
      sales_order_topics,
      new JSONKeyValueDeserializationSchema(true),
      prop)


    val sales_order_detailSource: FlinkKafkaConsumer011[ObjectNode] = new FlinkKafkaConsumer011[ObjectNode](
      sales_order_detail_topics,
      new JSONKeyValueDeserializationSchema(true),
      prop)



    //先获取流
    val sales_orderDStream: DataStream[ObjectNode] = env.addSource(sales_orderSource).setParallelism(1)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ObjectNode](Time
      .milliseconds(watermarkDelay)) {
      override def extractTimestamp(t: ObjectNode): Long = {
        val dataNode: JsonNode = t.get("value").get("data")
        val update_time: Long = MapUtil.getTimestamp(dataNode, "update_time")
        update_time
      }
    })
    val sales_order_detailDStream: DataStream[ObjectNode] = env.addSource(sales_order_detailSource).setParallelism(1)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ObjectNode](Time
        .milliseconds(watermarkDelay)) {
        override def extractTimestamp(t: ObjectNode): Long = {
          val dataNode: JsonNode = t.get("value").get("data")
          val update_time: Long = MapUtil.getTimestamp(dataNode, "update_time")
          update_time
        }
      })

    val SalesOrderDStream: DataStream[SalesOrder] = sales_orderDStream.map(new salesordermap)
    val SalesOrderDetailDStream: DataStream[SalesOrderDetail] = sales_order_detailDStream.map(new salesorderdetailmap)
   // SalesOrderDStream.print()
   // SalesOrderDetailDStream.print()

  //datastream转化为table
    val SalesOrderTable: Table = streamTableEnv.fromDataStream(SalesOrderDStream)
    val SalesOrderDetailTable: Table = streamTableEnv.fromDataStream(SalesOrderDetailDStream)

    streamTableEnv.registerTable("SalesOrderTable",SalesOrderTable)
    streamTableEnv.registerTable("SalesOrderDetailTable",SalesOrderDetailTable)



    //regular join
    // inner join
    """
      |SELECT
      |*
      |FROM SalesOrderDetailTable
      |INNER JOIN SalesOrderTable
      |ON SalesOrderDetailTable.sales_order_id = SalesOrderTable.id
      |
      |""".stripMargin
    //left outer join
    """
      |SELECT
      |*
      |FROM SalesOrderDetailTable
      |left JOIN SalesOrderTable
      |ON SalesOrderDetailTable.sales_order_id = SalesOrderTable.id
      |
      |""".stripMargin

    //right outer join
    """
      |SELECT
      |*
      |FROM SalesOrderDetailTable
      |right JOIN SalesOrderTable
      |ON SalesOrderDetailTable.sales_order_id = SalesOrderTable.id
      |
      |""".stripMargin

//full outer join
    """
      |SELECT
      |*
      |FROM SalesOrderDetailTable
      |full outer JOIN SalesOrderTable
      |ON SalesOrderDetailTable.sales_order_id = SalesOrderTable.id
      |
      |""".stripMargin



    val table: Table = streamTableEnv.sqlQuery(
      """
        |SELECT
        |*
        |FROM SalesOrderDetailTable
        |INNER JOIN SalesOrderTable
        |ON SalesOrderDetailTable.sales_order_id = SalesOrderTable.id
        |""".stripMargin)

//interval join
"""
      |SELECT
      |*
      |FROM SalesOrderDetailTable t1
      |JOIN SalesOrderTable t2
      |ON t1.sales_order_id = t2.id
      |and t2.update_time between t1.update_time - INTERVAL '4' MINUTE
      |and t1.update_time + INTERVAL '4' MINUTE
      |
      |""".stripMargin


    val value: DataStream[Row] = streamTableEnv.toAppendStream[Row](table)
value.print()

   /* //datastream转换为view
    streamTableEnv.createTemporaryView("resultView",tradeDStream)

    //table转换为view
    streamTableEnv.createTemporaryView("resultView",resultTable)*/








    env.execute()


  }

}
