package redis_connect.cust_connpo;


import redis_connect.socket.BatRedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;  // 单向队列

/**
 * Description: redis连接池
 */
public class RedisClientPool {

    private List<BatRedisClient> allObject;
    private LinkedBlockingQueue<BatRedisClient> linkedBlockingQueue;

    public RedisClientPool(String host,int port,int connectionCount){
        allObject = new ArrayList<>();
        this.linkedBlockingQueue = new LinkedBlockingQueue<>(10);
        for (int i = 0; i < connectionCount; i++) {
            BatRedisClient client = new BatRedisClient(host,port);
            linkedBlockingQueue.add(client);
            allObject.add(client);
        }
    }
    /**
     * 获取client连接
     */
    public BatRedisClient getClient(){
        try {
            return linkedBlockingQueue.take(); // or poll
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 将client归还到连接池
     */
    public void returnClient(BatRedisClient client){
        if(client == null){
            return;
        }
        if(!allObject.contains(client)){
            throw new IllegalStateException(
                    "Returned object not currently part of this pool");
        }
        try {
            linkedBlockingQueue.put(client);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
