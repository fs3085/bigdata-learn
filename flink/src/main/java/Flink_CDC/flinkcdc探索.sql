flink-Sql Client
通过sql-client.sh embedded命令进到flink-sql client
执行Sql语句
CREATE TABLE tb1 (
    id INTEGER NOT NULL,
    username varchar(50),
    password varchar(255),
    phone varchar(20),
    email varchar(255),
    create_time varchar(255)
    ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '172.16.0.23',
    'port' = '3306',
    'username' = 'root',
    'password' = 'xysh1234',
    'database-name' = 'davincidb',
    'table-name' = 'users'
    );

    CREATE TABLE tb2 (
    id INTEGER NOT NULL,
    username varchar(50),
    password varchar(255),
    phone varchar(20),
    email varchar(255),
    create_time varchar(255)
    ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '172.16.0.23',
    'port' = '3306',
    'username' = 'root',
    'password' = 'xysh1234',
    'database-name' = 'davincidb',
    'table-name' = 'users2'
    );

    --elasticsearch
    CREATE TABLE users_res (
      id INT,
      username STRING,
      PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'elasticsearch-7',
        'hosts' = 'http://localhost:9200',
        'index' = 'users_res'
    );


        insert into users_res
        select
        t1.id
        ,t2.username
        from tb1 t1
        left join tb2 t2
        on t2.id = t1.id;

    就能运行一个flink实时任务在yarn上面


MySQL CDC连接器:
https://github.com/ververica/flink-cdc-connectors/wiki/MySQL-CDC-Connector
https://zhuanlan.zhihu.com/p/243187428

    --获取指定索引数据
    curl -X GET "localhost:9200/users_res/_doc/1?pretty"