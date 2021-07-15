package sparksql_exe_tem.udfs.udaf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparksql_exe_tem.udfs.utils.TestUtil;

import java.util.ArrayList;

//按贷款账户24个月还款状态字符一一对应,按优先级取每个位置贷款还款状态最严重一个字符汇成综合还款状态；
//优先级：(数字越小表示优先级越高）
//'Z'=1,'D'=2,'G'=3,'7'=4,'6'=5,'5'=6,'4'=7,'3'=8,'2'=9,'1'=10,'0'=11,'N'=12,'*'=13,'C'=14,'/'=15,'#'=16
public class PASStatusA extends UserDefinedAggregateFunction {

    /**
     * StructType 表示此聚合函数的输入参数的数据类型。如果 UserDefinedAggregateFunction 需要两个类型为 DoubleType 和 LongType 的输入参数，则返回的 StructType 将如下所示
     * new StructType().add("doubleInput", DoubleType).add("longInput", LongType)
     * 此 StructType 的字段名称仅用于标识相应的输入参数。用户可以选择名称来标识输入参数
     **/
    @Override
    public StructType inputSchema() {
        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("str", DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }

    /**
     * StructType 表示聚合缓冲区中值的数据类型。例如，如果 UserDefinedAggregateFunction 的缓冲区有两个类型为 DoubleType 和 LongType 的值（即两个中间值），则返回的 StructType 将如下所示
     * new StructType().add("doubleInput", DoubleType).add("longInput", LongType)
     * 此 StructType 的字段名称仅用于标识相应的缓冲区值。用户可以选择名称来标识输入参数
     **/
    @Override
    public StructType bufferSchema() {
        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("max", DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }

    /**
     * 此 UserDefinedAggregateFunction 的返回值的 DataType。
     **/
    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }

    /**
     * 如果此函数是确定性的，即给定相同的输入，则始终返回相同的输出，则返回 true
     **/
    @Override
    public boolean deterministic() {
        return true;
    }


    /**
     * 初始化给定的聚合缓冲区，即聚合缓冲区的零值。
     * 约定应该是在两个初始缓冲区上应用合并函数应该只返回初始缓冲区本身，即 merge(initialBuffer, initialBuffer) 应该等于 initialBuffer。
     **/
    @Override
    //初始化为////////////////////////
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "////////////////////////");
    }

    /**
     * 使用来自输入的新输入数据更新给定的聚合缓冲区缓冲区。
     * 每个输入行调用一次。
     **/
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        String result = "";
        System.out.println(input.getString(0));
        if (input.getString(0) == null) {
            //输入为null
        } else {
            //拿到缓存的数据
            String buffername = buffer.getString(0);
            //拿到输入的数据
            String inputname = input.getString(0);
            //传入的参数为24个字符的字符串
            for (int i = 0; i < 24; i++) {
                //从右往左遍历，拿到字符，通过rank函数转换成数字
                int a1 = TestUtil.rank(buffername.substring(buffername.length() - i - 1, buffername.length() - i));
                int a2 = TestUtil.rank(inputname.substring(inputname.length() - i - 1, inputname.length() - i));
                //比较大小，选择大的字符
                if ((a1 - a2) < 0) {
                    result = inputname.substring(inputname.length() - i - 1, inputname.length() - i) + result;
                } else {
                    result = buffername.substring(buffername.length() - i - 1, buffername.length() - i) + result;
                }
            }
            //更新buffer
            buffer.update(0, result);
        }
    }

    /**
     * 合并两个聚合缓冲区并将更新的缓冲区值存储回 buffer1。
     * 当我们将两个部分聚合的数据合并在一起时会调用它。
     **/
    @Override
    //逻辑参考update
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String result = "";
        String buffer1name = buffer1.getString(0);
        String buffer2name = buffer2.getString(0);
        for (int i = 0; i < 24; i++) {
            int a1 = TestUtil.rank(buffer1name.substring(buffer1name.length() - i - 1, buffer1name.length() - i));
            int a2 = TestUtil.rank(buffer2name.substring(buffer2name.length() - i - 1, buffer2name.length() - i));
            if ((a1 - a2) < 0) {
                result = buffer2name.substring(buffer2name.length() - i - 1, buffer2name.length() - i) + result;
            } else {
                result = buffer1name.substring(buffer1name.length() - i - 1, buffer1name.length() - i) + result;
            }
        }
        buffer1.update(0, result);
    }

    /**
     * 根据给定的聚合缓冲区计算此 UserDefinedAggregateFunction 的最终结果。
     **/
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
