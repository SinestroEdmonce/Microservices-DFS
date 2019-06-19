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
