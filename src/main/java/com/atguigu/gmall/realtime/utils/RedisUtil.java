package com.atguigu.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool = null;

    public static Jedis getJedis(){
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);

            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);

            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig,"hadoop02",6379);

        }
        return jedisPool.getResource();
    }

//   public static void main(String[] args) {
//        Jedis jedis = getJedis();
//        System.out.println(jedis.ping());
//    }
}
