package com.bc.mysql2es

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

class dataversionmap extends MapFunction[ObjectNode,DataVersion]{
  override def map(t: ObjectNode): DataVersion = {

    val dataNode: JsonNode = t.get("value").get("data")


    val id: Long = MapUtil.getLong(dataNode, "id")
    val data_string: String = MapUtil.getString(dataNode, "data_string")
    val create_time: Long = MapUtil.getTimestamp(dataNode, "create_time")
    val update_time: Long = MapUtil.getTimestamp(dataNode, "update_time")

    val dateTimeFormat :SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


    DataVersion(id,data_string,dateTimeFormat.format(create_time),dateTimeFormat.format(update_time))




  }
}
