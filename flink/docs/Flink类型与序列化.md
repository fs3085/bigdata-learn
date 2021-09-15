

### 查看Flink的执行计划

在execute之前，我们可以添加以下代码：

```java
System.out.println(env.getExecutionPlan());
```

在main中编写的代码并不会执行数据处理，它只是在编写代码，构建Job Graph，只有当执行execute时，会在JVM中模拟一个mini flink cluster，然后提交JOB执行



MapReduce中如果使用POJO，是必须要实现Hadoop序列化的（Writable），要么就需要使用Hadoop中自带的序列化类，例如：IntWritable、LongWritable、Text等等

Flink中编写代码可以直接使用Java基础数据类型、POJO，无需显示实现任何的序列化框架，Flink并不是以JDK方式序列化，因为序列化的数据需要存放在内存中，使用JDK方式不利于内存管理。Flink自己实现了一套序列化架构（TypeInformat）

对于我们创建的任意的一个POJO类型，看起来它是一个普通的Java Bean，在Java中，使用Class可以用来描述该类型。但其实在Flink引擎中，它被描述为PojoTypeInfo，而PojoTypeInfo是**TypeInformation**的子类。

![img](https://pic3.zhimg.com/80/v2-3de8efcf88053ac5f48bf3c2636058c6_720w.jpg)

### TypeInformation

TypeInformation是Flink类型系统的核心类。Flink使用TypeInformation来描述所有Flink支持的数据类型，就像Java中的Class类型一样。每一种Flink支持的数据类型都对应的是TypeInformation的子类。

![img](https://pic3.zhimg.com/80/v2-0ca1804f7276654ce2192d07828dca26_720w.jpg)

例如：POJO类型对应的是PojoTypeInfo、基础数据类型数组对应的是BasicArrayTypeInfo、Map类型对应的是MapTypeInfo、值类型对应的是ValueTypeInfo。

除了对类型地描述之外，TypeInformation还提供了序列化的支撑。在TypeInformation中有一个方法：createSerializer方法，

![img](https://pic4.zhimg.com/80/v2-86e00d3fb13fe6516c274f2f28dee517_720w.jpg)

它用来创建序列化器，序列化器中定义了一系列的方法。其中，通过serialize和deserialize方法，可以将指定类型进行序列化。并且，Flink的这些序列化器会以稠密的方式来将对象写入到内存中。

![img](https://pic3.zhimg.com/80/v2-1a6d8696b23af5f603db57ad4590a3d2_720w.jpg)

Flink中也提供了非常丰富的序列化器。

![img](https://pic4.zhimg.com/80/v2-27d183afef4b2f83b8fc16b484aa09f7_720w.jpg)

基于Flink类型系统支持的数据类型进行编程时，Flink在运行时会推断出数据类型的信息，几乎是不需要关心类型和序列化的。



### Java类型擦除

在编译时，编译器能够从Java源代码中读取到完整的类型信息，并强制执行类型的约束，但生成class字节码时，会将参数化类型信息删除。这就是类型擦除。