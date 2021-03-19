# Flink 电商实时数仓
![image](https://user-images.githubusercontent.com/31069867/111018724-81a17380-83f5-11eb-9f2e-482f7839bca3.png)

![image](https://user-images.githubusercontent.com/31069867/111018782-dcd36600-83f5-11eb-820e-b09f991d9be7.png)

## 1. 日志数据采集 --SpringBoot 整合 Kafka + Nginx
## 2.业务数据库数据采集  -- Maxwell/Canal 
## 3. DWD 层数据准备 
![image](https://user-images.githubusercontent.com/31069867/111018965-2a9c9e00-83f7-11eb-9787-bb924767f9d2.png)

## 每层的职能
![image](https://user-images.githubusercontent.com/31069867/111018998-633c7780-83f7-11eb-84cb-c3f2d6ca0020.png)
![image](https://user-images.githubusercontent.com/31069867/111019016-7bac9200-83f7-11eb-90b6-0c698f577898.png)

## 4. 优化1：加入盘路缓存优化
订单宽表查询缓存， 未命中缓存查询数据库，同步缓存


## 5.异步IO查询
在 Flink 流处理过程中，经常需要和外部系统进行交互，用维度表补全事实表中的字段.
默认情况下，在 Flink 的 MapFunction 中，单个并行只能用同步方式去交互: 将请求发送到外部存储，IO 阻塞，等待请求返回，然后继续发送下一个请求。这种同步交互的方式往往在网络等待上就耗费了大量时间。
为了提高处理效率，可以增加 MapFunction 的并行度，但增加并行度就意味着更多的资源，并不是一种非常好的解决方式.
Flink 在 1.2 中引入了 Async I/O，在异步模式下，将 IO 操作异步化，单个并行可以连续发送多个请求，哪个请求先返回就先处理，从而在连续的请求间不需要阻塞式等待，大大提高了流处理效率。解决与外部系统交互时网络延迟成为了系统瓶颈的问题。

异步查询实际上是把维表的查询操作托管给单独的线程池完成，这样不会因为某一个查询造成阻塞，单个并行可以连续发送多个请求，提高并发效率.这种方式特别针对涉及网络 IO 的操作，减少因为请求等待带来的消耗
(1)封装线程池
（2）自定义维度查询接口
这个异步维表查询的方法适用于各种维表的查询，用什么条件查，查出来的结果如何合并到数据流对象中，需要使用者自己定义
（3）封装维度异步查询的函数类 DimAsyncFunction
核心的类是 AsyncDataStream，这个类有两个方法一个是有序等待（orderedWait），一个是无序等待（unorderedWait）
