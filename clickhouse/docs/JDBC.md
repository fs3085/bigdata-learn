JDBC表引擎不仅可以对接MySQL数据库，还能够与PostgreSQL等数据库。为了实现JDBC连接，ClickHouse使用了**clickhouse-jdbc-bridge**的查询代理服务。

首先我们需要下载clickhouse-jdbc-bridge，然后按照ClickHouse的github中的步骤进行编译，编译完成之后会有一个**clickhouse-jdbc-bridge-1.0.jar**的jar文件，除了需要该文件之外，还需要JDBC的驱动文件，本文使用的是MySQL，所以还需要下载MySQL驱动包。将MySQL的驱动包和**clickhouse-jdbc-bridge-1.0.jar**文件放在了/opt/softwares路径下，执行如下命令：

```shell
vim datasource-config.txt (后缀名不重要)
datasource.myclickhouse=clickhouse://node02:8123/default?user=default&password=
datasource.mysql=mysql://localhost:3306/?user=root&password=000000


nohup java -jar clickhouse-jdbc-bridge-1.0.jar  --driver-path .  --listen-host 0.0 --datasources datasource-config.txt &
```



```mysql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=000000', 'tidi_viz', 'user')

SELECT *  FROM jdbc( 'datasource://mysql',  'tidi_viz', 'user');
```

