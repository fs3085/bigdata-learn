**软件下载**

软件下载地址：https://www.elastic.co/cn/downloads/past-releases/elasticsearch-7-8-0 

 **软件安装**

**1)** **解压软件**

将下载的软件解压缩

\# 解压缩

```shell
tar -zxvf elasticsearch-7.8.0-linux-x86_64.tar.gz -C /opt/module
```

\# 改名

```shell
mv elasticsearch-7.8.0 es-cluster
```

将软件分发到其他节点：linux2, linux3

2) **创建用户**

因为安全问题，Elasticsearch 不允许 root 用户直接运行，所以要在每个节点中创建新用户，在 root 用户中创建新用户

```shell
useradd es #新增 es 用户

passwd es #为 es 用户设置密码

userdel -r es #如果错了，可以删除再加

chown -R es:es /opt/module/es-cluster #文件夹所有者
```

**3)** **修改配置文件**

修改/opt/module/es/config/elasticsearch.yml 文件，分发文件

```shell
# 加入如下配置

#集群名称

cluster.name: cluster-es

#节点名称，每个节点的名称不能重复

node.name: node-1

#ip 地址，每个节点的地址不能重复

network.host: linux1

#是不是有资格主节点

node.master: true

node.data: true

http.port: 9200

# head 插件需要这打开这两个配置

http.cors.allow-origin: "*"

http.cors.enabled: true

http.max_content_length: 200mb

#es7.x 之后新增的配置，初始化一个新的集群时需要此配置来选举 master

cluster.initial_master_nodes: ["node-1"]

#es7.x 之后新增的配置，节点发现

discovery.seed_hosts: ["linux1:9300","linux2:9300","linux3:9300"]

gateway.recover_after_nodes: 2

network.tcp.keep_alive: true

network.tcp.no_delay: true

transport.tcp.compress: true

#集群内同时启动的数据任务个数，默认是 2 个

cluster.routing.allocation.cluster_concurrent_rebalance: 16

#添加或删除节点及负载均衡时并发恢复的线程个数，默认 4 个

cluster.routing.allocation.node_concurrent_recoveries: 16

#初始化数据恢复时，并发恢复线程的个数，默认 4 个

cluster.routing.allocation.node_initial_primaries_recoveries: 16
```

修改/etc/security/limits.conf ，分发文件

```shell
# 在文件末尾中增加下面内容

es soft nofile 65536

es hard nofile 65536
```

修改/etc/security/limits.d/20-nproc.conf，分发文件

```shell
# 在文件末尾中增加下面内容

es soft nofile 65536

es hard nofile 65536

* hard nproc 4096

# 注：* 带表 Linux 所有用户名称
```

修改/etc/sysctl.conf

```shell
# 在文件中增加下面内容

vm.max_map_count=655360
```

重新加载

```shell
sysctl -p 
```

**启动软件**

分别在不同节点上启动 ES 软件

```shell
cd /opt/module/es-cluster
```

```shell
#启动

bin/elasticsearch

\#后台启动

bin/elasticsearch -d 
```

