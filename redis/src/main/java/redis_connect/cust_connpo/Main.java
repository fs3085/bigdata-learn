package redis_connect.cust_connpo;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Description: 测试redis连接池
 */
public class Main {
    public static void main(String[] args) {
        ExecutorService pool = Executors.newCachedThreadPool();
        RedisClientPool redisClientPool = new RedisClientPool("localhost",6379,10);
        for (int i = 0; i < 20; i++) {
            pool.execute(new ClientPoolRunable(redisClientPool,"123"+i));
        }
    }
}
