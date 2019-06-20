package com.joey.sa.mdfs.datanode.Database;


/**
 * @projectName Microservices-DFS
 * @fileName DatabaseFileNotFoundException
 * @auther Qiaoyi Yin
 * @time 2019-06-20 12:44
 * @function Database File NotFound Exception, used as an error reporter when files do not exist
 */
public class DatabaseFileNotFoundException extends DatabaseException {

    public DatabaseFileNotFoundException(String message) {
        super(message);
    }

    public DatabaseFileNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
