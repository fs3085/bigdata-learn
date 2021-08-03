package redis_connect.cust_connpo;

/**
 * Description: 使用自定义redis连接池
 */

import redis_connect.socket.BatRedisClient;

public class ClientPoolRunable implements Runnable {
    private RedisClientPool pool;
    private String value;

    public ClientPoolRunable(RedisClientPool pool, String value) {
        this.pool = pool;
        this.value = value;
    }

    @Override
    public void run() {
        BatRedisClient client = pool.getClient();
        client.set("ant",value);
        pool.returnClient(client);
    }
}
