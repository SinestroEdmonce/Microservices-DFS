package com.joey.sa.mdfs.datanode.FileSystem;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * @projectName Microservices-DFS
 * @fileName BlockInfo
 * @auther Qiaoyi Yin
 * @time 2019-06-20 12:12
 * @function Block Information, used to record the information of block
 */
@Entity
public class BlockInfo {
    @Id
    @GeneratedValue
    private Long blockId;

    // Block Attributes
    private String fileName;
    private long blockSize;

    public String getFileName(){
        return this.fileName;
    }

    public long getBlockSize(){
        return this.blockSize;
    }

    public BlockInfo(String fileName, long blockSize){
        this.fileName = fileName;
        this.blockId = blockSize;
    }
}
