### 声明版本表 [#](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/concepts/versioned_tables/#声明版本表)

在 Flink 中，定义了主键约束和事件时间属性的表就是版本表。

```sql
-- 定义一张版本表
CREATE TABLE product_changelog (
  product_id STRING,
  product_name STRING,
  product_price DECIMAL(10, 4),
  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) 定义主键约束
  WATERMARK FOR update_time AS update_time   -- (2) 通过 watermark 定义事件时间              
) WITH (
  'connector' = 'kafka',
  'topic' = 'products',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'debezium-json'
);
```

行 `(1)` 为表 `product_changelog` 定义了主键, 行 `(2)` 把 `update_time` 定义为表 `product_changelog` 的事件时间，因此 `product_changelog` 是一张版本表。

**注意**: `METADATA FROM 'value.source.timestamp' VIRTUAL` 语法的意思是从每条 changelog 中抽取 changelog 对应的数据库表中操作的执行时间，强烈推荐使用数据库表中操作的 执行时间作为事件时间 ，否则通过时间抽取的版本可能和数据库中的版本不匹配。



### COMPUTED COLUMN(计算列)

可以通过PROCTIME()函数定义处理时间属性，语法为`proc AS PROCTIME()`。



### 水位线

水位线定义了表的事件时间属性，其语法为:

```
WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
```

其中`rowtime_column_name`表示表中已经存在的事件时间字段，值得注意的是，该事件时间字段必须是TIMESTAMP(3)类型，即形如`yyyy-MM-dd HH:mm:ss`,如果不是这种形式的数据类型，需要通过定义计算列进行转换。

Flink提供了许多常用的水位线生成策略：

- 严格单调递增的水位线：语法为

```
WATERMARK FOR rowtime_column AS rowtime_column
```

即直接使用时间时间戳作为水位线

- - 递增水位线:语法为
    WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND
  - 乱序水位线：语法为
    WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit
    -- 比如，允许5秒的乱序
    WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '5' SECOND



## 时态表函数 [#](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/concepts/versioned_tables/#时态表函数)

时态表函数是一种过时的方式去定义时态表并关联时态表的数据，现在我们可以用时态表 DDL 去定义时态表，用[时态表 Join](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/sql/queries/joins/#时态表-join) 语法去关联时态表。

时态表函数和时态表 DDL 最大的区别在于，时态表 DDL 可以在纯 SQL 环境中使用但是时态表函数不支持，用时态表 DDL 声明的时态表支持 changelog 流和 append-only 流但时态表函数仅支持 append-only 流。

为了访问时态表中的数据，必须传递一个[时间属性](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/concepts/time_attributes/)，该属性确定将要返回的表的版本。 Flink 使用[表函数](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/functions/udfs/#表值函数)的 SQL 语法提供一种表达它的方法。

定义后，*时态表函数*将使用单个时间参数 timeAttribute 并返回一个行集合。 该集合包含相对于给定时间属性的所有现有主键的行的最新版本。



### 定义时态表函数 [#](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/concepts/versioned_tables/#定义时态表函数)

以下代码段说明了如何从 append-only 表中创建时态表函数。

Java

```java
import org.apache.flink.table.functions.TemporalTableFunction;
(...)

// 获取 stream 和 table 环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// 提供一个汇率历史记录表静态数据集
List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
ratesHistoryData.add(Tuple2.of("Euro", 114L));
ratesHistoryData.add(Tuple2.of("Yen", 1L));
ratesHistoryData.add(Tuple2.of("Euro", 116L));
ratesHistoryData.add(Tuple2.of("Euro", 119L));

// 用上面的数据集创建并注册一个示例表
// 在实际设置中，应使用自己的表替换它
DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, $("r_currency"), $("r_rate"), $("r_proctime").proctime());

tEnv.createTemporaryView("RatesHistory", ratesHistory);

// 创建和注册时态表函数
// 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
tEnv.registerFunction("Rates", rates);                                                              // <==== (2)
```

Scala

行`(1)`创建了一个 `rates` [时态表函数](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/concepts/versioned_tables/#时态表函数)， 这使我们可以在[ Table API ](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/tableapi/#joins)中使用 `rates` 函数。

行`(2)`在表环境中注册名称为 `Rates` 的函数，这使我们可以在[ SQL ](https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/dev/table/sql/queries/#joins)中使用 `Rates` 函数。



使用版本表就是为了获取（当前proctime或者当前event_time对应的最新的一份数据），因为主键和时间唯一确定一条数据。版本表关联语法是这样的：A LEFT JOIN B FOR SYSTEM TIME AS OF A.{proctime | rowtime} ON A.key = B.key,(注意这里可以使用的event_time或者proctime),所以如果需要使用event_time,我们就需要定义watermark.

