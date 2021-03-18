package com.atguigu.gmall.realtime.bean;

import java.math.BigDecimal;
import java.util.Date;

import lombok.Data;
//订单实体类
@Data
public class OrderInfo {
    Long id;
    String consignee;
    String consignee_tel;
    BigDecimal total_amount;
    String order_status;
    Long user_id;
    String payment_way;
    String delivery_address;
    String order_comment;
    String out_trade_no;
    String trade_body;
    String expire_time;
    String create_time;
    String operate_time;
    String process_status;
    String tracking_no;
    String parent_order_id;
    String img_url;
    Long province_id;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    BigDecimal feight_fee_reduce;
    String refundable_time;
    Date create_date; // 把其他字段处理得到
    Long create_ts;
}
