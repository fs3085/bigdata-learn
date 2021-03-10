package com.ywf.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Properties;


@Configuration
@EnableTransactionManagement
public class RedisConfig {
    Log logger = LogFactory.getLog(this.getClass());

    @Value("${redis.master}")
    String redisMaster;

    @Value("${redis.password}")
    String redisPassword;

    @Value("${redis.sentinel1}")
    String redisSentinel1;

    @Value("${redis.sentinel2}")
    String redisSentinel2;

    @Value("${redis.sentinel3}")
    String redisSentinel3;

    @Value("${redis.testOnBorrow}")
    String redisTestOnBorrow;

    @Value("${redis.maxIdle}")
    String redisMaxIdle;

    @Value("${redis.maxWaitMillis}")
    String redisMaxWaitMillis;

    @Value("${redis.maxTotal}")
    String redisMaxTotal;

    @Bean(name = "redisProps")
    public Properties RedisProps() {
        Properties props = new Properties();
        props.setProperty("redis.master", redisMaster);
        props.setProperty("redis.password", redisPassword);
        props.setProperty("redis.sentinel1", redisSentinel1);
        props.setProperty("redis.sentinel2", redisSentinel2);
        props.setProperty("redis.sentinel3", redisSentinel3);
        props.setProperty("redis.testOnBorrow", redisTestOnBorrow);
        props.setProperty("redis.maxIdle", redisMaxIdle);
        props.setProperty("redis.maxWaitMillis", redisMaxWaitMillis);
        props.setProperty("redis.maxTotal", redisMaxTotal);
        return props;
    }

}
