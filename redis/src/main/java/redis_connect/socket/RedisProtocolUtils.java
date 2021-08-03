package redis_connect.socket;


/**
 * Description: redis协议工具类
 */
public class RedisProtocolUtils {
    public static final String DOLIER = "$";
    public static final String ALLERTSTIC = "*";
    public static final String CRLE = "\r\n";

    public static byte[] buildRespByte(Command command, byte[]... bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append(ALLERTSTIC).append(bytes.length + 1).append(CRLE);
        sb.append(DOLIER).append(command.name().length()).append(CRLE);
        sb.append(command.name()).append(CRLE);
        for (byte[] arg : bytes) {
            sb.append(DOLIER).append(arg.length).append(CRLE);
            sb.append(new String(arg)).append(CRLE);
        }
        return sb.toString().getBytes();
    }

    /**
     * redis set，get命令
     */
    public enum Command {
        SET, GET
    }
}
