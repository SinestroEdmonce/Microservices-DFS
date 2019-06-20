package com.joey.sa.mdfs.namenode.Error;


/**
 * @projectName Microservices-DFS
 * @fileName ErrorPrinter
 * @auther Qiaoyi Yin
 * @time 2019-06-19 14:02
 * @function TODO
 */


public class ErrorPrinter {

    private static final String[] errorMessage = {
            "Error: PATH (%s) not exist!",
            "Error: FILE (%s) not exist!",
            "Error: DataNodes are not enough for replicas! (%s)",
            "Error: FILE (%s) already exists!",
            "Error: DataNode (%s) is offline!",
            "Error: DataNode (%s) is never online before!",
            "Warning: DataNode (%s) was removed!",
            "Error: Fail to find replicas for BLOCK (%s)!",
            "Error: Fail to find an appropriate DataNode for FILE (%s)!",
            "Warning: New DataNode (%s) is discovered!",
            "Error: DataNode (%s) already exists!",
            "Warning: New DataNode (%s) is registered!",
            "Error: Pulse from (%s)",
            "Warning: Eureka registry is now available!",
            "Warning: Eureka server launches!"
    };

    private int errorNum;
    private String info;

    public ErrorPrinter(int errorNum, String info){
        this.errorNum = errorNum;
        this.info = info;
    }

    public void setErrorNum(int errorNum){
        this.errorNum = errorNum;
    }

    public void setInfo(String info){
        this.info = info;
    }

    public void printError(){
        StringBuilder error = new StringBuilder(this.errorMessage[this.errorNum-1]);

        // Replace the {} with error information
        System.err.println(String.format(error.toString(), this.info));
    }

}
