package redis_connect.socket;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Description: Socket网络连接到redis,需要使用redis协议进行连接
 */
public class Connetion {
    private String host;
    private int post;
    private Socket socket;           // 线程不安全
    private InputStream inputStream;
    private OutputStream outputStream;

    public Connetion(String host, int post) {
        this.host = host;
        this.post = post;
    }

    /**
     * 判断连接是否已经建立,判断连接是不是初始化好了，或者连接没有断开
     */
    public boolean isConnection(){
        if(socket !=null && inputStream !=null && socket.isClosed()){
            return true;
        }
        try {
            socket = new Socket(host,post);
            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 发送命令
     */
    public String sendCommand(byte[] command){
        if(isConnection()){
            try {
                // 客户端先写数据
                outputStream.write(command);
                // 读取服务端响应
                byte[] res = new byte[1024];
                int length = 0;
                while ((length=inputStream.read(res))>0){
                    return new String(res,0,length);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}
