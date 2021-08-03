package redis_connect.socket;

/**
 * Description:
 */
public class ClientRunalbe implements Runnable {

    private BatRedisClient client;
    private String value;

    public ClientRunalbe(BatRedisClient client, String value) {
        this.client = client;
        this.value = value;
    }

    @Override
    public void run() {
        client.set("ant", value);
    }
}