package redis_connect.socket;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Description: redis连接客户端含redis协议和socket
 */
public class BatRedisClient {
    private Connetion connetion;
    public BatRedisClient(String host,int port){
        connetion = new Connetion(host,port);
    }

    public String set(String key,String value){
        String result = connetion.sendCommand(
                RedisProtocolUtils.buildRespByte(RedisProtocolUtils.Command.SET,key.getBytes(),value.getBytes()));
        System.out.println("Thread name:"+Thread.currentThread().getName()
                +"[result]:"+result.replace("\r\n","")+"[value]:"+value);
        return result;
    }

    public String get(String key){
        String result = connetion.sendCommand(
                RedisProtocolUtils.buildRespByte(RedisProtocolUtils.Command.GET,key.getBytes(),key.getBytes()));
        return result;
    }

    public static void main(String[] args) {
        BatRedisClient client = new BatRedisClient("localhost",6379);
        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i < 20; i++) {
            // 存在现象: 当前线程读取到redis返回给其他线程的响应数据
            pool.execute(new ClientRunalbe(client,"123"+i));
        }
    }
}
