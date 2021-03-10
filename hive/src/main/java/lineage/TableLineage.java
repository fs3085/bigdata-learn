package lineage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Stack;
import java.util.TreeSet;

/**
 * 表级别血缘
 */
public class TableLineage implements NodeProcessor {
    TreeSet<String> inputTableList = new TreeSet<String>();
    TreeSet<String> outputTableList = new TreeSet<String>();
    TreeSet<String> withTableList = new TreeSet<String>();

    public TreeSet<String> getInputTableList() {
        return inputTableList;
    }

    public TreeSet<String> getOutputTableList() {
        return outputTableList;
    }

    public TreeSet<String> getWithTableList() {
        return withTableList;
    }

    public Object process(Node node, Stack<Node> stack, NodeProcessorCtx nodeProcessorCtx, Object... objects) throws SemanticException {
        ASTNode astNode = (ASTNode) node;
        // System.out.println(astNode);
        switch (astNode.getToken().getType()) {
            // create table
            case HiveParser.TOK_CREATETABLE: {
                String createName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) astNode.getChild(0));
                outputTableList.add(createName);
                break;
            }
            // insert table
            case HiveParser.TOK_TAB: {
                String insertName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) astNode.getChild(0));
                outputTableList.add(insertName);
                break;
            }
            // from table
            case HiveParser.TOK_TABREF: {
                ASTNode tabTree = (ASTNode) astNode.getChild(0);
                String fromName = tabTree.getChildCount() == 1 ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) :
                                  BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." +
                                      BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(1));
                inputTableList.add(fromName);
                break;
            }
            // with 语句
            case HiveParser.TOK_CTE: {
                for (int i = 0; i < astNode.getChildCount(); i++) {
                    ASTNode tmp = (ASTNode) astNode.getChild(i);
                    String withName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tmp.getChild(0));
                    withTableList.add(withName);
                    break;
                }
            }
        }
        return null;
    }

    public String dealWithSql(String sql) {
        // sql = sql.replace("`", "");
        sql = sql.replace(";", "");
        return sql;
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException, IOException {
        query = dealWithSql(query);
        ParseDriver parseDriver = new ParseDriver();
        try {
            Configuration hiveConf = new Configuration();
            hiveConf.set("hive.support.quoted.identifiers", "column"); // 处理 select x as `字段x` from.. 这种含有``的sql
            hiveConf.set("_hive.hdfs.session.path", "hdfs"); // 伪造HDFS_SESSION_PATH_KEY以绕过代码检查 // TODO:这里源码为什么要做检验？
            hiveConf.set("_hive.local.session.path", "local"); // 伪造LOCAL_SESSION_PATH_KEY以绕过代码检查
            Context ctx = new Context(hiveConf);
            ASTNode tree = parseDriver.parse(query, ctx);
            // System.out.println(tree.dump()); // 打印出整个抽象语法树
            while (tree.getToken() == null && tree.getChildCount() > 0) {
                tree = (ASTNode) tree.getChild(0);
            }
            inputTableList.clear();
            outputTableList.clear();
            withTableList.clear();

            LinkedHashMap<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

            Dispatcher ruleDispatcher = new DefaultRuleDispatcher(this, rules, null);
            GraphWalker graphWalker = new DefaultGraphWalker(ruleDispatcher);

            ArrayList topNodes = new ArrayList();
            topNodes.add(tree);
            graphWalker.startWalking(topNodes, null);
        } catch (Exception e) {
            System.out.println("======================== 解析SQL出错 ===========================");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SemanticException, ParseException, IOException {
        // String query = "create TABLE test.customer_kpi as SELECT base.datekey,base.clienttype, count(distinct base.userid) buyer_count FROM ( SELECT p.datekey datekey, p.userid userid, c.clienttype FROM detail.usersequence_client c JOIN fact.orderpayment p ON p.orderid = c.orderid JOIN default.user du ON du.userid = p.userid WHERE p.datekey = 20131118 ) base GROUP BY base.datekey, base.clienttype";
        // String query = "with q1 as ( select key from src where key = '5'), q2 as ( select key from with1 a inner join with2 b on a.id = b.id) insert overwrite table temp.dt_mobile_play_d_tmp2 partition(dt='2018-07-17') select * from q1 cross join q2";
        // String query = "insert into qc.tables_lins_cnt partition(dt='2016-09-15') select a.x from (select x from cc group by x) a left  join yy b on a.id = b.id left join (select * from zz where id=1) c on c.id=b.id";
        // String query ="from (select id,name from xx where id=1) a insert overwrite table  dsl.dwm_all_als_active_d partition (dt='main') select id group by id insert overwrite table  dsl.dwm_all_als_active_d2 partition (dt='main') select name group by name";
        // String query = "SELECT user_id, username from ods_touna.dw_user limit 10";

        String query = "";
        File file = new File("E:\\JavaWorkshop\\bigdata-learn\\hive\\src\\main\\resources\\xxx.sql");
        long length = file.length();
        byte[] bytes = new byte[(int) length];
        try {
            FileInputStream inputStream = new FileInputStream(file);
            inputStream.read(bytes);
            inputStream.close();
            query = new String(bytes);
            System.out.println(query);
        } catch (IOException e) {
            e.printStackTrace();
        }

        TableLineage tableLineage = new TableLineage();
        tableLineage.getLineageInfo(query);

        System.out.println("input:" + tableLineage.getInputTableList());
        System.out.println("output:" + tableLineage.getOutputTableList());
        System.out.println("with:" + tableLineage.getWithTableList());


    }
}
