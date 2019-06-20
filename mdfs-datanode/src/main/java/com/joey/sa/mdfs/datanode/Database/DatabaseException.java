package com.joey.sa.mdfs.datanode.Database;


/**
 * @projectName Microservices-DFS
 * @fileName DatabaseException
 * @auther Qiaoyi Yin
 * @time 2019-06-20 12:44
 * @function Database Exception, used as an exception reporter
 */
public class DatabaseException extends RuntimeException {

    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}

