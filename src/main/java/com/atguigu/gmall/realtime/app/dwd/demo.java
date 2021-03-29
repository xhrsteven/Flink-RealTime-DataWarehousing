package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.UserInfo;
import org.json4s.Serialization;

import java.util.ArrayList;
import java.util.List;

public class demo {
    public static void main(String[] args) {

        UserInfo userInfo1 = new UserInfo();
        userInfo1.setUser_id(1L);
        userInfo1.setId(2L);

        UserInfo userInfo2 = new UserInfo();
        userInfo2.setId(2L);
        userInfo2.setUser_id(3L);

        List<UserInfo> list = new ArrayList<UserInfo>();
        list.add(userInfo1);
        list.add(userInfo2);
        System.out.println(JSON.toJSONString(list));
    }
}
