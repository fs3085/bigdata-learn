**关于BIO和NIO**

BIO和NIO是老生常谈的话题，一般谈起IO，主要分为三种：BIO、NIO和AIO。在Linux上主要是前两种比较多，AIO目前支持还不够完善



在Java语言中，这三种IO都是支持的，其中AIO被称作NIO2，是JDK1.7引入的，不过就实际使用来说，还是前两种用得比较广泛。大名鼎鼎的网络框架netty，就是基于NIO的，不过netty的NIO模型不是基于JDK的，而是自己实现了一套，但本质上来说，都是IO多路复用。这里要澄清一些概念上的东西，很容易发生混淆：

1. NIO就是异步IO吗？

不对，BIO的全称是同步阻塞IO，NIO是同步非阻塞IO，AIO才是异步非阻塞IO。所以关键点在是否阻塞，就目前的技术来说，真正异步的IO应用还不广泛。

2. select、poll、epoll和NIO之间的关系

实际上更多的关注的是select、poll和epoll之间的区别，三者其实都是NIO的实现手段。通常聊到NIO会联系到epoll，但两者并不等价。

NIO相比于BIO，具有更好的并发能力，相同的服务器在同一时间能能够处理更多的请求，更节省资源，不过相应的会带来开发成本的提升，所以通常情况下，只有底层框架或者性能敏感的场景才会考虑使用NIO。



**Vert.x和netty之间的关系**

Eclipse Vert.x是Eclipse基金会下面的一个开源项目，Vert.x的基本定位是一个事件驱动的编程框架，通过Vert.x使用者可以用相对低的成本就享受到NIO带来的高性能。Vert.x对异步处理和事件驱动做了一些合理抽象，第三方可以基于这些抽象提供扩展组件，目前市面上也已经有很多这一类组件了。

netty是Vert.x底层使用的通讯组件，Vert.x为了最大限度的降低使用门槛，刻意屏蔽掉了许多底层netty相关的细节，比如ByteBuf、引用计数等等。不过如果你细心的话，实际上还是能找到很多netty的影子。

熟悉netty的同学应该知道，在netty中有两个EventLoop，一个BossEventLoop和一个WorkerEventLoop，分别负责负责监听Socket事件和处理对应的请求。在Vert.x中，这两个EventLoop通过一定的方法都可以获取到。不过，通常来说更加关注的是WorkerEventLoop，因为用户自定义的代码跑在这个EventLoop上。Vert.x对这个EventLoop作了扩展，除了处理服务器IO事件以外，用户自定义的事件也可以基于Vert.x进行处理。



Vert.x提供了非常多的原生组件来达到异步的效果，比如vertx-redis-client，vertx-kafka-client，vertx-mysql-client等，这些组件并不是简单地基于原本的第三方jar包+executeBlocking包装实现，而是直接从TCP层面实现了原本的协议，同时带来了真正的异步。







