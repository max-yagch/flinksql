package com.bc.mysql2es

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.net.ntp.TimeStamp
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

class salesorderdetailmap extends MapFunction[ObjectNode,SalesOrderDetail]{
  override def map(t: ObjectNode): SalesOrderDetail = {

    val dataNode: JsonNode = t.get("value").get("data")


    val id: Long = MapUtil.getLong(dataNode, "id")
    val sales_order_id: Long = MapUtil.getLong(dataNode, "sales_order_id")
    val data_version: Long = MapUtil.getLong(dataNode, "data_version")
    val create_time: Long = MapUtil.getTimestamp(dataNode, "create_time")
    val update_time: Long = MapUtil.getTimestamp(dataNode, "update_time")

    val dateTimeFormat :SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    SalesOrderDetail(id,sales_order_id,data_version,dateTimeFormat.format(create_time),new Timestamp(update_time))
  }
}
