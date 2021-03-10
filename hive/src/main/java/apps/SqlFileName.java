package apps;

import java.io.File;

public class SqlFileName {
    public static void main(String[] args) {
        File file = new File("D:\\MT-bank\\new_stg\\bigdata-warehouse\\标准产品\\卡核心数据集市");
        File[] files = file.listFiles();

        for(File filesqlname : files) {
            String strsqlnamepath = filesqlname.toString();
            String[] split = strsqlnamepath.split("\\\\");
            //System.out.println(split[6]);
            String strsqlname = split[split.length-1].split("\\.")[0];

            String s = "insert into table test.compare select\n" +
                "\"${strsqlname}\" as tablename\n" +
                ",t.counts\n" +
                "from\n" +
                "(select\n" +
                "count(*) as counts\n" +
                "from\n" +
                "(select \n" +
                "distinct \n" +
                "*\n" +
                "from \n" +
                "(select `(id|wcl_etltime)?+.+` from wcl_dwh_deva.${strsqlname}\n" +
                "union all\n" +
                "select `(id|wcl_etltime)?+.+` from wcl_dwh_devb.${strsqlname})ua)ds)t;\n" +
                "\n" +
                "insert into table test.compare select\n" +
                "\"${strsqlname}\" as tablename\n" +
                ",t.counts\n" +
                "from\n" +
                "(select \n" +
                "count(*) as counts\n" +
                "from wcl_dwh_deva.${strsqlname})t";

            String sql = s.replace("${strsqlname}", strsqlname);
            System.out.println("---------------"+strsqlname+"--------------");
            System.out.println(sql+";");
            System.out.println("---------------end--------------");
            System.out.println();
            System.out.println();
            System.out.println();

        }
    }
}
