package com.joey.sa.mdfs.datanode.Database;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;


/**
 * @projectName Microservices-DFS
 * @fileName DatabaseProperty
 * @auther Qiaoyi Yin
 * @time 2019-06-20 12:41
 * @function Database Property, used to store a database's attributes
 */
@ConfigurationProperties("storage")
public class DatabaseProperty {

    @Value(value = "${server.port}")
    private Integer location;

    public String getLocation() {
        return location.toString();
    }
}
