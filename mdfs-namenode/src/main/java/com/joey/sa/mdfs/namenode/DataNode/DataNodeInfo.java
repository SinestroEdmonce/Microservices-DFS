package com.joey.sa.mdfs.namenode.DataNode;

import javafx.util.Pair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;


/**
 * @projectName Microservices-DFS
 * @fileName DataNodeInfo
 * @auther Qiaoyi Yin
 * @time 2019-06-19 11:29
 * @function Data Node Information, used to store data node URL and describe whether the node is available
 */
public class DataNodeInfo {

    // Variable that is used to store every file block on the given data node
    private HashSet<Pair<String, Long>> blocks = new HashSet<>();

    private String addressURL;

    private boolean availability = true;

    public DataNodeInfo(String address){
        this.addressURL = address;
    }

    public void setAvailability(boolean isAvailable){
        this.availability = isAvailable;
    }

    // Available or not
    public boolean isAvailable(){
       return this.availability;
    }

    // Add file blocks to the given data node
    public void addBlock(String fileName, long indexOfBlock){
        this.blocks.add(new Pair<>(fileName, indexOfBlock));
    }


    // Remove a given file block
    public void removeBlock(String fileName, long indexOfBlock){
        this.blocks.remove(new Pair<>(fileName, indexOfBlock));
    }


    // Obtain the block list
    public List<Pair<String, Long>> getBlocks(){
        return new LinkedList<>(this.blocks);
    }
}
