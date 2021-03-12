package Split_SelectAndSide_Outputs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class SplitSelectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv= StreamExecutionEnvironment.getExecutionEnvironment();
        /*为方便测试 这里把并行度设置为1*/
        senv.setParallelism(1);

        DataStream<String> sourceData = senv.readTextFile("D:\\sideOutputTest.txt");

        DataStream<PersonInfo> personStream = sourceData.map(new MapFunction<String, PersonInfo>() {
            @Override
            public PersonInfo map(String s) throws Exception {
                String[] lines = s.split(",");
                PersonInfo personInfo = new PersonInfo();
                personInfo.setName(lines[0]);
                personInfo.setProvince(lines[1]);
                personInfo.setCity(lines[2]);
                personInfo.setAge(Integer.valueOf(lines[3]));
                personInfo.setIdCard(lines[4]);
                return personInfo;
            }
        });
        //这里是用spilt-slect进行一级分流
  //      SplitStream<PersonInfo> splitProvinceStream = personStream.split(new OutputSelector<PersonInfo>() {
  //          @Override
  //          public Iterable<String> select(PersonInfo personInfo) {
  //              List<String> split = new ArrayList<>();
  //              if ("shandong".equals(personInfo.getProvince())) {
  //                  split.add("shandong");
  //              } else if ("jiangsu".equals(personInfo.getProvince())) {
  //                  split.add("jiangsu");
  //              }
  //              return split;
  //          }
  //      });
        
        
        
   //     DataStream<PersonInfo> shandong = splitProvinceStream.select("shandong");
   //     DataStream<PersonInfo> jiangsu = splitProvinceStream.select("jiangsu");

        //定义流分类标识  进行一级分流
        OutputTag<PersonInfo> shandongTag = new OutputTag<PersonInfo>("shandong") {};
        OutputTag<PersonInfo> jiangsuTag = new OutputTag<PersonInfo>("jiangsu") {};

        SingleOutputStreamOperator<PersonInfo> splitProvinceStream = personStream.process(new ProcessFunction<PersonInfo, PersonInfo>() {
            @Override
            public void processElement(PersonInfo person, Context context, Collector<PersonInfo> collector) throws Exception {
                if ("shandong".equals(person.getProvince())) {
                    context.output(shandongTag, person);
                } else if ("jiangsu".equals(person.getProvince())) {
                    context.output(jiangsuTag, person);
                }
            }
        });

        DataStream<PersonInfo> shandongStream = splitProvinceStream.getSideOutput(shandongTag);
        DataStream<PersonInfo> jiangsuStream = splitProvinceStream.getSideOutput(jiangsuTag);

        /*下面对数据进行二级分流，我这里只对山东的这个数据流进行二级分流，江苏流程也一样*/
        OutputTag<PersonInfo> jinanTag = new OutputTag<PersonInfo>("jinan") {};
        OutputTag<PersonInfo> qingdaoTag = new OutputTag<PersonInfo>("qingdao") {};

        SingleOutputStreamOperator<PersonInfo> cityStream = shandongStream.process(new ProcessFunction<PersonInfo, PersonInfo>() {
            @Override
            public void processElement(PersonInfo person, Context context, Collector<PersonInfo> collector)
                throws Exception {
                if ("jinan".equals(person.getCity())) {
                    context.output(jinanTag, person);
                } else if ("qingdao".equals(person.getCity())) {
                    context.output(qingdaoTag, person);
                }
            }
        });

        DataStream<PersonInfo> jinan = cityStream.getSideOutput(jinanTag);
        DataStream<PersonInfo> qingdao = cityStream.getSideOutput(qingdaoTag);

        jinan.map(new MapFunction<PersonInfo, String>() {
            @Override
            public String map(PersonInfo personInfo) throws Exception {
                return personInfo.toString();
            }
        }).print("山东-济南二级分流结果:");
        qingdao.map(new MapFunction<PersonInfo, String>() {
            @Override
            public String map(PersonInfo personInfo) throws Exception {
                return personInfo.toString();
            }
        }).print("山东-青岛二级分流结果:");

//        /*一级分流结果*/
//        shandongStream.map(new MapFunction<PersonInfo, String>() {
//            @Override
//            public String map(PersonInfo personInfo) throws Exception {
//                return personInfo.toString();
//            }
//        }).print("山东分流结果:");
//        /*一级分流结果*/
//        jiangsuStream.map(new MapFunction<PersonInfo, String>() {
//            @Override
//            public String map(PersonInfo personInfo) throws Exception {
//                return personInfo.toString();
//            }
//        }).print("江苏分流结果: ");
        senv.execute();
    }
}
