# 关闭防火墙
systemctl stop firewalld
systemctl disable firewalld
iptables -F
vi /etc/selinux/config
SELINUX=disabled

# 免密
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
service sshd restart
ssh-copy-id -i ~/.ssh/id_rsa.pub root@192.166.201.34

useradd tidb && \
passwd tidb

visudo
tidb ALL=(ALL) NOPASSWD: ALL

使用tidb用户
tar xzvf tidb-community-server-v5.2.0-linux-amd64.tar.gz /home/tidb
sh tidb-community-server-v5.2.0-linux-amd64/local_install.sh
source ~/.bash_profile


使用root权限，具体修改/etc/ssh/sshd_config文件下面的参数配置：
MaxSessions 20 
改完后重启sshd：
service sshd restart 

vi topo.yaml
# # Global variables are applied to all deployments and used as the default value of 
# # the deployments if a specific deployment value is missing. 
global: 
 user: "tidb" 
 ssh_port: 22 
 deploy_dir: "/home/tidb/tidb-deploy" 
 data_dir: "/home/tidb/tidb-data" 
 
# # Monitored variables are applied to all the machines. 
monitored: 
 node_exporter_port: 9100 
 blackbox_exporter_port: 9115 
 
server_configs: 
 tidb: 
   log.slow-threshold: 300 
 tikv: 
   readpool.storage.use-unified-pool: false 
   readpool.coprocessor.use-unified-pool: true 
 pd: 
   replication.enable-placement-rules: true 
   replication.location-labels: ["host"] 
 tiflash: 
   logger.level: "info" 
 
pd_servers: 
 - host: 192.166.201.34 
 
tidb_servers: 
 - host: 192.166.201.34 
 
tikv_servers: 
 - host: 192.166.201.34 
   port: 20160 
   status_port: 20180 
   config: 
     server.labels: { host: "logic-host-1" } 
 
# - host: 192.166.201.34 
#   port: 20161 
#   status_port: 20181 
#   config: 
#     server.labels: { host: "logic-host-2" } 
 
# - host: 192.166.201.34 
#   port: 20162 
#   status_port: 20182 
#   config: 
#     server.labels: { host: "logic-host-3" } 
 
tiflash_servers: 
 - host: 192.166.201.34 
 
 tiup cluster deploy mytidb-cluster v5.2.0 /home/tidb/topo.yaml --user root -p 
 
 
 # 修改密码
 SET PASSWORD FOR 'root'@'%' = 'xysh1234';
 
 
 https://database.51cto.com/art/202101/639665.htm
 
 参数：
Transaction is too large	事务限制	performance.txn-total-size-limit	事务大小限制	(1073741824)1G	(10737418240)10G
Out of Memory Quota！	内存不够	mem-quota-query	单个SQL语句可用的最大内存	(1073741824)1G	(10737418240)10G

# 修改配置
 tiup cluster list
 tiup cluster display mytidb-cluster
 tiup cluster edit-config mytidb-cluster
 tiup cluster reload mytidb-cluster
 
 # 添加服务
 vi scale-out.yaml
 monitoring_servers:
  - host: 192.166.201.34
grafana_servers:
  - host: 192.166.201.34
alertmanager_servers:
  - host: 192.166.201.34
tiup cluster scale-out mytidb-cluster /home/tidb/scale-out.yaml
 
 

 
 
 
 
 