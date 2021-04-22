package com.bc.mysql2es

import java.math.BigDecimal
import java.text.SimpleDateFormat

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode


object MapUtil {






  def getString(dataNode :JsonNode,columeName :String): String ={
    try {
      dataNode.get(columeName).asText()
    } catch {
      case nullPointerException: NullPointerException =>null
    }
  }



  def getLong(dataNode :JsonNode,columeName :String): Long ={
    try {
      dataNode.get(columeName).asLong(-999999999)
    } catch {
      case nullPointerException: NullPointerException => -999999999
    }
  }



  def getByte(dataNode :JsonNode,columeName :String): Byte ={
    try {
      val byte: Byte = dataNode.get(columeName).asInt(-128).toByte
      byte
    } catch {
      case nullPointerException: NullPointerException =>  -128
    }
  }



  def getInteger(dataNode :JsonNode,columeName :String): Integer ={
    try {
      val integer: Integer = new Integer(dataNode.get(columeName).asInt(-999999999))
      integer
    } catch {
      case nullPointerException: NullPointerException =>  null
    }
  }



  def getTimestamp(dataNode :JsonNode,columeName :String):Long={
    try {
      val dateTimeFormat :SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timeString: String = dataNode.get(columeName).asText()
      dateTimeFormat.parse(timeString).getTime

    } catch {
      case _ =>  -1
    }
  }

  def getDate(dataNode :JsonNode,columeName :String):Long={
    try {
      val dateFormat :SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val timeString: String = dataNode.get(columeName).asText()
      dateFormat.parse(timeString).getTime

    } catch {
      case _ =>  -1
    }
  }


  def getBigDecimal(dataNode :JsonNode,columeName :String):BigDecimal={
    try {
      val timeString: String = dataNode.get(columeName).asText()
      scala.math.BigDecimal(timeString).bigDecimal

    } catch {
      case _ =>  null
    }
  }





}
