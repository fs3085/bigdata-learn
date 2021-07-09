一：**Metadata&\**Metastore\****

**Metadata**即元数据。元数据包含用Hive创建的database、table、表的字段等元信息。元数据存储在关系型数据库中。如hive内置的Derby、第三方如MySQL等。

**Metastore**即元数据服务，作用是：客户端连接metastore服务，metastore再去连接MySQL数据库来存取元数据。有了metastore服务，就可以有多个客户端同时连接，而且这些客户端不需要知道MySQL数据库的用户名和密码，只需要连接metastore 服务即可。

**HiveMetaStore**:是为Apache hive提供元数据的元数据服务，它属于Apache hive开源项目，目前已经可以作为Standalone提供服务，且不限于Hive，第三方服务也可以使用其作为元数据库服务。

二：**Schema**

Hive MetaStore作为元数据库，其Schema基本遵循数据库系统的Schema标准，其Schema层次关系为：Catalog->Database->[Schema->]Table->Partition->Field，其中Schema这层目前还没有正式的使用起来的，至少在Hive中并没用用起来，不过对于Catalog目前只使用了默认Catalog，而且在Hive Sql语法中也没有相关语法定义，但是其作为元数据库的话，不限于Hive，对于其他系统完全可以利用其Schema，且目前都在喊数据目录，何为数据目录，Catalog就是数据目录。

另外Hive3.x之前的版本支持Index能里，但是3.x版本开始停用Index能里，所以3.x版本的Hive MetaStore Schema里虽有Index的表结构相关的定义，但是并没有提供相关的API。

三：**架构**

![image.png](https://forum-img.huaweicloud.com/data/attachment/forum/202010/31/174525dnx5hsasesphzd1c.png)

其中：

1、Hive MetaStore使用Thrift作为RPC Server对外提供服务

2、Hive MetaStore使用DataNucleus（一种JDO中间件）作为数据库中间件来同数据库交互

3、Hive MetaStore目前支持多种关系数据库作为最终的元数据存储：berby、mssql、mysql、oracle、postgres

4、Hive MetaStore代码里大量使用了ThreadLocal来保证每个线程的资源都是独享的互不影响的

5、Hive MetaStore正在研发元数据缓存机制来提升效率，避免每次都需要同数据库交互

6、Hive MetaStore由于是无状态的（非本地缓存），所以支持分布式多实例部署，但是其自身提供的本地缓存不支持分布式，所以在分布式多实例部署的情况下，为了提升效率，华为内部已经有团队将Redis引入作为分布式缓存，这样就可以在分布式多实例的部署中引入缓存机制

7、Hive MetaStore设计的元数据Schema层级及深度较高，所以，对于像表这样的对象，如果要获取表详情，其内部同数据库交互频次很高，在某种程度对性能是有影响的

8、Hive MetaStore提供工具类用来初始化、升级元数据库

9、Hive MetaStore目前仍然基于HDFS，所以脱离了HDFS是不能正常工作的

10、Hive MetaStore目前有两种运行形态：独立的Standalone模式+内嵌在HiveServer中local模式

11、Hive MetaStore无论客户端还是Server端都通过反射的机制利用Proxy实现了重试机制