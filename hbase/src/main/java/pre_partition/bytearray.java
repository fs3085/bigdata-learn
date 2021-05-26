package pre_partition;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class bytearray {
    public static void main(String[] args) {

        byte[][] bytes = new byte[7][];

        for(int i = 0; i < 7; i++) {
            String leftPad = StringUtils.leftPad(i + "", 4, "0");

            System.out.println(leftPad);


            bytes[i] = Bytes.toBytes(leftPad+"|");



//            System.out.println(Bytes.toBytes(leftPad+"|").toString());
        }

        System.out.println(bytes[0][3]);
        System.out.println(bytes[1][3]);
        System.out.println(bytes[2][3]);
        System.out.println(bytes[3][3]);
        System.out.println(bytes[4][3]);
        System.out.println(bytes[5][3]);
        System.out.println(bytes[6][3]);


//        System.out.println("asdadasdasddadad".hashCode()%5);

    }

}
