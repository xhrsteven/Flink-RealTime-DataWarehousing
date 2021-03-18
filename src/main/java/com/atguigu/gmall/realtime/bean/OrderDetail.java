package com.atguigu.gmall.realtime.bean;
//订单明细实体类
/**
 *
 */

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderDetail {
    Long id;
    Long order_id ;
    Long sku_id;
    String sku_name;
    String img_url;
    BigDecimal order_price ;
    Long sku_num ;
    String create_time;
    String source_type;
    Long source_id;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}
