## 一、窗口函数的概念

首先，需要认识到，窗口函数并不是只有 hive 才有的，SQL 语法标准中，就有窗口函数。

并且 mysql，oracle等数据库都实现了窗口函数。

而 hive 自带的窗口函数功能，则是对原有 hive sql 语法的补充和加强。

那么什么时候，会用到窗口函数？

举两个小栗子：

- 排名问题：每个部门按业绩排名
- topN 问题：找出每个部门排名前 N 的员工进行奖励

面对这类需求，就需要使用窗口函数了。

窗口函数的基本语法如下：

```
<窗口函数> over (partition by <用于分组的列名>
                order by <用于排序的列名>)
```

那么语法中的窗口函数的位置，可以放以下两种函数：

1）专用窗口函数，包括后面要讲到的 rank，dense_rank，row_number 等专用窗口函数

2）聚合函数，如 sum，avg，count，max，min 等

因为窗口函数是对 where 或者 group by 子句处理后的结果进行操作，所以窗口函数原则上只写在 select 子句中。

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJS99URlGHbkz7kKfiblFfndrBicSB6ico0GHnoerEuwc2a6gOJfTlM1h4EA/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

## 二、实战窗口函数

我们以一个用户月消费明细表，作为例子，来实战各个窗口函数的功能 首先建表

```sql
drop table tempon.t_user_cost;
create external table if not exists tempon.t_user_cost(
name string comment '用户名',
date string comment '月份',
cost int comment '花费'
) comment '用户花费表' 
row format delimited fields terminated by ","
location '/tmp/person_cost'
```

数据为：

```text
jack,2015-01-01,10
tony,2015-01-02,15
jack,2015-02-03,23
tony,2015-01-04,29
jack,2015-01-05,46
jack,2015-04-06,42
tony,2015-01-07,50
jack,2015-01-08,55
mart,2015-04-08,62
mart,2015-04-09,68
neil,2015-05-10,12
mart,2015-04-11,75
neil,2015-06-12,80
mart,2015-04-13,94
```

### 1、over 关键字的理解

```
select name,count(1) over() 
from tempon.t_user_cost
```

这里的 over() 中既没有 partition by，也没有 order by，表示不分区（自然也不排序），也就是把全局数据分一个区，结果输出到每一行上。

可以看到运行结果中，还是 14 行，并且每行都有一个统计值。

> 聚合函数是会缩减行数的，而窗口函数则不会，就可以直观看到，截止到本行数据，统计结果是多少。

### 2、partition by 子句

也叫查询分区子句，将数据按照边界值分组，而over()之前的函数在每个分组内执行。

```sql
select name,
       date,
       cost,
sum(cost) over(partition by month(date)) 
from tempon.t_user_cost;
```

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJSnRecnIfvfXsFTqnG64IKv0WQEj9k5ibh3tZv5icJDZPCot477QPowsag/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

可以看到，数据是对月份（month(date)）来分区的，并且对于每个月都统计了  sum(cost) 值。（由于没有 order by 子句，sum 函数是对于所有数据的累加）。

### 3、order by 子句

order by 子句，是对某一个字段分区，对分区内的另一个字段进行排序。

排好序后，对于不同的聚合函数效果不一样。

- 如果和 sum 函数一起使用，就是按照排序，逐行累加
- 如果和 count 函数一起使用，就是按照排序，计数累加

```sql
select name,
       date,
       cost,
       sum(cost) over(partition by month(date) order by cost) 
from tempon.t_user_cost;
```

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJS2w2m34eD5eHvDu8d4IbnT6uzGAX4fNlEklic4PaEYYdpd1iaBL4o5KbA/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

和 count 函数一起使用，则是逐行计数累加

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJSViaWlCMPZ9152EXJfUf0hsToHsyJhS6ERahrxOe0mNquxiaWjPxhUiaRA/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

### 4、window子句

（不同的窗口互不影响，自己算自己的）

window是为了更加细粒度的划分

如果只使用了partition by子句，未指定order by的话，我们的聚合是分组内的聚合；

如果使用了order by子句，未使用window子句，默认从起点到当前行；

如果使用了下面的语法，那么当前行可以随意指定统计规则

```
rows between keyword1 and keyword2
```

- 当前行：current row
- 往前n行：n preceding
- 往后n行：n following
- 往前无限行：unbouded preceding
- 往后无限行：unbouded following

如下面的 SQL

```sql
select name,date,cost,
sum(cost) over() sample1,  -- 所有行累加
sum(cost) over(partition by name) sample2, -- 按照name相加
sum(cost) over(partition by name order by cost) sample3, --按照name累加
sum(cost) over(partition by name order by cost rows between unbounded preceding and current row) sample4, --和sample3一样的效果
sum(cost) over(partition by name order by cost rows between 1 preceding and current row) sample5, -- 当前行和上一行相加
sum(cost) over(partition by name order by cost rows between 1 preceding and 1 following) sample6, -- 上一行、当前行、后一行相加
sum(cost) over(partition by name order by cost rows between current row and unbounded following) sample7 -- 当前行到末尾
from tempon.t_user_cost;
```

### 5、row_number() 和 rank() 和 dense_rank()

这三个函数是为了排序，但是有区别

```sql
select name,
       date,
       cost,
       row_number() over(partition by name order by cost),
       rank()       over(partition by name order by cost),
       dense_rank() over(partition by name order by cost)
  from tempon.t_user_cost;
```

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJStwR7bFVdnquOk8omgU1vA1Ea9iacicqI3ZvLr230AYVy8I5hsXqzwx5g/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

row_number() 是无脑排序

rank() 是相同的值排名相同，相同值之后的排名会继续加，是我们正常认知的排名，比如学生成绩。

dense_rank()也是相同的值排名相同，接下来的排名不会加。不会占据排名的坑位。

### 6、lag函数 和 lead函数

lag()函数是在窗口内，在指定列上，取上N行的数据，并且有默认值。没有设置默认值的话，为null

lag(dt,1,'1990-01-01') 就是在窗口分区内，往上取 1 行的数据，填到本行中。如果是第一行，则取 1990-01-01

lead(dt,1,'1990-01-01') 就是在窗口分区内，往下取1行的数据，填到本行中。如果是第一行，则取 1990-01-01

```sql
select name,date,cost,
  lag(date,1,'1990-01-01') over(partition by name order by date),
  lag(date,2,'1990-01-01') over(partition by name order by date),
  lead(date,1,'1990-01-01') over(partition by name order by date)
    from tempon.t_user_cost
```

第一个参数是列名，第二个参数是取上多少行的数据，第三个参数是默认值

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJSwFDE9YJqb4iaXAM2LI3cq46DQMdBX4XTNfoRHVGibiabcaGUXrF4iaHASg/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

### 7、first_value() 和 last_value()

```sql
select name,
         date,
         cost,
         first_value(date) over(partition by name order by date),
         last_value(date) over(partition by name order by date)
    from tempon.t_user_cost;
```

![图片](https://mmbiz.qpic.cn/mmbiz_png/yofhibCRConBLSibAqhxOA3XWvbTf2YLJSthzS1bwGA0c01N6EYVN3dSX5n8yDVsibEEuc8ouaKfdmYVmg3ic3PJng/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)

当前分区的第一个值和最后一个值