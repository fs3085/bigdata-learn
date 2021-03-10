public class test {
    public static void main(String[] args){
        String data_date = "20200101";
        String sqls="";
        String cal_mth = data_date.substring(0,6);
        System.out.println(cal_mth);
        sqls = sqls.replace("${data_date}", data_date);
        sqls = sqls.replace("$cal_mth", cal_mth);
    }
}
