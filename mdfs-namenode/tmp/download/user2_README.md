# mdfs
A distributed file system built with microservices
## 实现的功能
使用Spring Boot和Spring Cloud Eureka实现了一个基于微服务架构的分布式文件系统，通过与name node结点通信实现文件的上传、下载和删除功能。<Br>
实现了弹性扩展，并且使用一致性hash策略实现负载均衡，在增加或删除数据结点时会动态进行数据迁移。
## 接口说明
name node结点提供如下形式的接口：
- Get http://localhost:8761/allFiles 返回所有文件及文件大小
- Get http://localhost:8761/fileOnNodes 返回所有文件块在数据结点上的分布情况
- Get http://localhost:8761/${Dir}/{File} 下载Dir路径下的File文件
- Put http://localhost:8761/${Dir}/ 将文件上传到Dir路径下，若路径不存在则会自动创建文件夹
- Delete http://localhost:8761/${Dir}/{File} 删除Dir路径下的File文件

## 使用说明
### Name Node
启动name node：
```shell
$ cd namenode
$ mvn spring-boot:run
```
name node的配置文件为application.yml，部分参数如下
```properties
block:
  default-size: 40000
  default-replicas: 2
load-balancer:
  num-visual-node: 31
test-mode: true
```
其中，test-mode设为true时，会在namenode端输出更多调试信息。
### Data Node
启动data node（最好不少于两个，以存放数据副本）:
```Shell
$ cd datanode
$ mvn spring-boot:run -Dserver.port=8081
$ mvn spring-boot:run -Dserver.port=8082
$ mvn spring-boot:run -Dserver.port=8083
```

## 运行截图
上传若干个文件后：

![所有文件](ScreenShots/all_files.png)

文件块的分布：

![blocks](ScreenShots/blocks.png)

增加一个结点(8084端口):

![blocks2](ScreenShots/blocks2.png)

删除一个结点(8082端口):

![blocks3](ScreenShots/blocks3.png)