package sparksql_exe_tem.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtils {

    public static String readFromLocalFile(String path) throws IOException {
        StringBuilder sqls = new StringBuilder();

        int hasRead = 0;
        byte[] bytes = new byte[1024];
        FileInputStream inputStream = new FileInputStream(path);
        while ((hasRead = inputStream.read(bytes))>0) {
            String part = new String(bytes, 0, hasRead);
            sqls.append(part);
        }
        return sqls.toString();
    }

    public static String readFromHdfsFile(String path) {
        String line = null;
        StringBuilder sql = new StringBuilder();
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fin = fs.open(new Path(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while ((line = br.readLine()) != null){
                sql.append(line).append('\n');
            }
            br.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return sql.toString();
    }
}
