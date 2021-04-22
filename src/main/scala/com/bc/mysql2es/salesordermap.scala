package com.bc.mysql2es

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

class salesordermap extends MapFunction[ObjectNode,SalesOrder]{
  override def map(t: ObjectNode): SalesOrder = {
    val dataNode: JsonNode = t.get("value").get("data")


    val id: Long = MapUtil.getLong(dataNode, "id")
    val data_version: String = MapUtil.getString(dataNode, "data_version")
    val create_time: Long = MapUtil.getTimestamp(dataNode, "create_time")
    val update_time: Long = MapUtil.getTimestamp(dataNode, "update_time")

    val dateTimeFormat :SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    SalesOrder(id,data_version,dateTimeFormat.format(create_time),dateTimeFormat.format(update_time))

  }
}
