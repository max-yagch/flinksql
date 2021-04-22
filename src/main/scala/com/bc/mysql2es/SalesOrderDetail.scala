package com.bc.mysql2es

import java.sql.Timestamp


case class SalesOrderDetail(
                             id:Long,
                             sales_order_id :Long,
                             data_version:Long,
                             create_time:String,
                             update_time:Timestamp

                           )
