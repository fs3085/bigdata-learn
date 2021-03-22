mysql8.0版本以下不支持使用lag,lead函数
**LAG**
LAG(col,n,DEFAULT) over(partition by 字段 order by 字段)用于统计窗口内往上第n行值
第一个参数为列名，
第二个参数为往上第n行（可选，默认为1），
第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）

**LEAD**
与LAG相反
LEAD(col,n,DEFAULT) over(partition by 字段 order by 字段) 用于统计窗口内往下第n行值
第一个参数为列名，
第二个参数为往下第n行（可选，默认为1），
第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）

**用mysql变量实现lag函数功能**

```mysql
SELECT base.id
,base.before_name
,base.name
,base.time
FROM (SELECT IF(@id = t.id, @lagname := @NAME, @lagname := '') AS before_name
,@id := t.id AS tid
,@NAME := t.name AS tafter_name
,t.*
FROM (SELECT *
FROM t_order
ORDER BY id
,TIME) t
,(SELECT @lagname := NULL
,@id      := 0
,@NAME    := NULL) r) base;
```

mysql变量：
为变量赋初始值

```mysql
(SELECT @lagname := NULL
,@id      := 0
,@NAME    := NULL) r
```

源表数据

```mysql
(SELECT *
FROM t_order
ORDER BY id
,TIME) t
```

每一行遍历一次，变量的值可以记录

```mysql
SELECT IF(@id = t.id, @lagname := @NAME, @lagname := '') AS before_name
,@id := t.id AS tid
,@NAME := t.name AS tafter_name
,t.*
```

