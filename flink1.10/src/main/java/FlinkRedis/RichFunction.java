package FlinkRedis;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import kafkajoinmysql.ActivityBean;
import redis.clients.jedis.Jedis;

public class RichFunction extends RichMapFunction<String, String> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取全局参数
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String host = params.getRequired("redis.host");
        //String pwd = params.getRequired("redis.pwd");
        int db = params.getInt("redis.db", 0);
        jedis = new Jedis(host, 6379, 5000);
        //jedis.auth(pwd);
        jedis.select(db);
    }

    @Override
    public String map(String line) throws Exception {

        String hashkeyvalue = jedis.hget("word_count", line);
        //System.out.println(hashkeyvalue);
        if(hashkeyvalue == null) {
            jedis.hset("word_count", line, "1");
        } else {
            jedis.hset("word_count",line,(Integer.valueOf(hashkeyvalue)+1)+"");
        }
        return line;
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }
}








