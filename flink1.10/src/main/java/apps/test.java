package apps;


import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;


//import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class test {

    private static final Logger LOG = LoggerFactory.getLogger(test.class);
    private static final String LOCAL_ADDRESS = "127.0.0.1";
    public static String getIpAddress(String interfaceName){
        try{
            Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
            while(nis.hasMoreElements()){
                NetworkInterface networkInterface = nis.nextElement();
                Enumeration<InetAddress> ias = networkInterface.getInetAddresses();
                if(1>0) {
                    System.out.println("------->");
                    while (ias.hasMoreElements()) {
                        InetAddress inetAddress = ias.nextElement();
                        System.out.println(inetAddress);
                        if (inetAddress instanceof Inet4Address &&
                            !inetAddress.getHostAddress().equals(LOCAL_ADDRESS)) {
                            System.out.println(inetAddress.getHostAddress() + "||" + networkInterface.getName());
                            return inetAddress.getHostAddress();
                        }
                    }
                }
            }
        } catch (SocketException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    public static void main(String[] args) {
        getIpAddress("127.0.0.1");
    }
}
