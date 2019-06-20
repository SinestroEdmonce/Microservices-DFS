# Microservices-DFS

基于微服务架构的基础分布式文件系统，通过``Spring-boot``和``Spring-cloud``分别实现微服务和服务注册中心（Service和Eureka Server服务注册中心）

## 功能实现

``Microservices-DFS``已实现的功能：  

- [x] 上传不同用户的文件（文件名可以相同，但用户名需要不同）
- [x] 下载系统上的文件
- [x] DataNode服务可弹性扩展，启动一个DataNode服务后，NameNode可发现并将其纳入系统
- [x] DataNode下线时NameNode将迁移该DataNode上的数据到其他活跃结点上
- [x] NameNode使用一致性Hash策略，新DataNode被注册时将会动态维持负载均衡
- [ ] 为服务NameNode的前端页面

## 接口说明

``Microservices-DFS``提供如下接口：

- ``Get /fileList`` 返回``Microservices-DFS``上所有文件名和其大小
- ``Get /blocksOnNode`` 返回``Microservices-DFS``上文件块在DataNode上的分布情况
- ``Get /${directory}/${filename}`` 下载指定文件
- ``Del /${directory}/${filename}`` 删除``${directory}``文件夹下的指定文件
- ``Put /${directory}/`` 上传文件至``${directory}``文件夹下

## 使用说明

- NameNode结点：

    - 启动NameNode：
        ```bash
        cd mdfs-namenode
        mvn spring-boot:run
        ```

    - NameNode的配置文件为``application.yml``：
    
        ```yml
        server:
            port: 8761

        eureka:
            instance:
                hostname: localhost
            client:
                registerWithEureka: false
                fetchRegistry: false
            serviceUrl:
                defaultZone: http://${eureka.instance.hostname}:${server.port}/eureka/

        spring:
            application:
                name: mdfs-namenode

        block:
            default-size: 40000
            default-replica-num: 2

        load-balance:
            virtual-node-num: 31

        test-mode: true
        ```

- DataNode结点：

    - 启动DataNode：

        ```bash
        cd mdfs-datanode
        mvn spring-boot:run -Dserver.port=8090
        mvn spring-boot:run -Dserver.port=8091
        mvn spring-boot:run -Dserver.port=8092
        ```

## ``Microservices-DFS``运行时刻截图