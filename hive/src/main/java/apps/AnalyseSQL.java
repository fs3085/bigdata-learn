package apps;

import lineage.TableLineage;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class AnalyseSQL {

    public static void main(String[] args) throws IOException, SemanticException, ParseException {
        TableLineage tableLineage = new TableLineage();

        File file = new File("D:\\MT-bank\\new_stg\\bigdata-warehouse\\标准产品\\卡核心数据集市");
        File[] files = file.listFiles();

        for(File filesql : files){
            String strsql = filesql.toString();
            //System.out.println(strsql);

            // String sqltext = FileUtils.readFromFile("E:\\JavaWorkshop\\bigdata-learn\\hive\\src\\main\\resources\\sqls.txt");
            String sqltext = readFromFile(strsql);

            String[] sqls = sqltext.split(";");
            for (int i = 0; i < sqls.length; i++) {
                String sql = sqls[i];
                //System.out.println(i + "\tsql:" + sql);
                boolean needToParse = !sql.replace("\r\n", "")
                                          .replace("\n", "")
                                          .replace("\t", "")
                                          .replace(" ", "").isEmpty();
                if (needToParse) {
                    tableLineage.getLineageInfo(sql);
                    System.out.println("input:" + tableLineage.getInputTableList()
                                           + "\toutput:" + tableLineage.getOutputTableList()
                                           + "\twith:" + tableLineage.getWithTableList());
                }
            }
        }
    }

    public static String readFromFile(String path) throws IOException {
        String sqls = "";

        int hasRead = 0;
        byte[] bytes = new byte[1024];
        FileInputStream inputStream = new FileInputStream(path);
        while ((hasRead = inputStream.read(bytes))>0) {
            String part = new String(bytes, 0, hasRead);
            sqls = sqls + part;
        }
        return sqls;
    }
}
