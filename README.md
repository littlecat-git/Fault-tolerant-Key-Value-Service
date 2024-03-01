# 项目介绍
该项目是一个分布式、容错的键值存储服务，基于Raft一致性算法实现。  
该服务允许客户端发送三种不同的RPC请求：Put(key, value)、Append(key, arg)和Get(key)。服务维护一个简单的键值对数据库，允许客户端执行对数据库的写入和读取操作。  
通过Raft算法实现的复制状态机，确保在系统中大多数服务器存活并能够通信的情况下，服务能够继续处理客户端请求，即使出现故障或网络分区。

# 项目特点
分布式架构：采用分布式架构，将键值对存储服务部署在多台服务器上，实现数据的分布式存储和处理。  
容错性：通过Raft一致性算法实现容错性，确保在系统中一部分服务器宕机或出现网络故障的情况下，系统仍能够正常运行，保障数据的可用性和一致性。  
线性可扩展：采用Raft算法作为一致性协议，具有良好的可扩展性，能够适应不同规模的系统和负载。  
客户端友好：通过RPC接口，客户端可以方便地与服务进行通信，并执行对数据库的读写操作。  
数据一致性：确保在并发请求下，对数据库的修改操作能够按照一定的顺序执行，保证数据一致性和线性化。  

# 遇到的困难
## 死锁
要提交快照的时候由于没有人取走applyCh通道里面的东西，导致死锁。  

具体解释：  
2D的测试代码中在日志达到一定大小时会调用snapshot，该函数需要申请rf.mu这个互斥锁。而在提交普通的日志条目时，错误地没有先释放锁，导致snapshot无法进行下去，相关的进程卡在rf.mu这个锁上，无法完成快照，更无法处理applyCh通道的下一个日志条目。这导致了向通道中提交日志条目也会因为applyCh已满而被阻塞。
## 如何定位错误
![image](https://github.com/littlecat-git/Fault-tolerant-Key-Value-Service/assets/82521506/86de46e3-f98e-4b56-b91a-c5a5e2560f6a)
可以看到打印的日志中出现了“whe： u4"的信息，就可以推知：相关的错误发现在被u4标记的代码处。  
![image](https://github.com/littlecat-git/Fault-tolerant-Key-Value-Service/assets/82521506/14028eba-cf9f-411e-82d9-107dee2738b3)
在访问日志具体条目的代码处，传入相应的标记，这样当调用getEntry函数失败时，能快速定位目标。
## 截断日志导致的边界情况
### u4报错
![image](https://github.com/littlecat-git/Fault-tolerant-Key-Value-Service/assets/82521506/80713c39-58e2-4b5e-99a4-019daeea30fb)
发生u4报错，定位到相应代码。  

在leader方，由于prevLogIndex处的日志条目被截取，小于rf.log.start(), 在运行getEntry函数时发生报错。  

解决方案：  

1.设置prevLogIndex = rf.log.start()  

2.应发送给follower的日志条目被删除，直接发送快照给follower。  
### u1报错

![image](https://github.com/littlecat-git/Fault-tolerant-Key-Value-Service/assets/82521506/a8568322-a17d-4d52-89fc-afe21a66c794)
发生u1报错，出错的点在于follower这边：leader方发送出去的时候prevLogIndex没有低于其自身的start，故没有发送快照，但是接收方收到日志条目之后，由于已经截断了日志，并不能匹配prevLogIndex。  

显然接收方对这种情况也需要处理，并不能仅仅返回个error就完事了。  
解决方案：  
设置XLen为start（）+1， 即日志中的第一个条目，leader在收到回应的时候会执行nextIndex[i] = XLen, 这样就将nextIndex设置为follower方的日志第一个条目。  
### 复制条目失效

![image](https://github.com/littlecat-git/Fault-tolerant-Key-Value-Service/assets/82521506/368f605b-8dcd-48c3-a3ac-3b04e7754817)
当prevLogIndex等于start时候，由于不匹配可能导致添加条目无法成功。  
解决方案：
在截断日志的时候需要设置占位的条目的term为snapshot.term；无论是安装快照的时候，还是自己截断日志、生成快照的时候。  




