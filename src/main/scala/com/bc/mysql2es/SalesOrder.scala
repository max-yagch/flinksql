package com.bc.mysql2es

import java.sql.Timestamp

case class SalesOrder(
                       id:Long,
                       data_version:String,
                       create_time:String,
                       update_time:String
                     )
