## *conf/canal.properties* 

```shell
# tcp, kafka, RocketMQ
canal.serverMode = kafka
```



## *conf/example/instance.properties*

```shell
canal.instance.master.address=127.0.0.1:3306 # 监控的数据库地址
canal.instance.dbUsername=canal # 监控的数据库用户名
canal.instance.dbPassword=canal
canal.instance.connectionCharset = UTF-8
canal.instance.defaultDatabaseName =test # 选择监控的数据库
canal.instance.filter.regex=.*\\..* # 白名单，选择监控哪些表
canal.instance.filter.black.regex=  # 黑名单，选择不监控哪些表
canal.mq.topic=example  ##将数据发送到指定的topic
```



## *example*

`example`文件夹,一个example就代表一个instance实例.而一个instance实例就是一个消息队列,所以这里可以将文件名改为example1,同时再复制出来一个叫example2.(命名可以使用监听的数据库名)，配置canal.properties

```shell
canal.destinations = example1,example2
```

