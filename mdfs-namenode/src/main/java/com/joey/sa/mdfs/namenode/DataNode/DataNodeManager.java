package com.joey.sa.mdfs.namenode.DataNode;

import javafx.util.Pair;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;



/**
 * @projectName Microservices-DFS
 * @fileName DataNodeManager
 * @auther Qiaoyi Yin
 * @time 2019-06-19 11:29
 * @function DataNode Manager, used to store the structure of data nodes and organize their information
 */
public class DataNodeManager {
    private Map<String, DataNodeInfo> mapOfAddressAndNodeInfo = new HashMap<>();

    // Add new data node
    public void addDataNode(String address){
        DataNodeInfo dataNodeInfo = new DataNodeInfo(address);
        this.mapOfAddressAndNodeInfo.put(address, dataNodeInfo);
    }

    // Remove data node from the list
    public void removeDataNode(String address){
        this.mapOfAddressAndNodeInfo.remove(address);
    }

    // Obtain the block list
    public List<Pair<String, Long>> getBlocks(String address){
        return this.mapOfAddressAndNodeInfo.get(address).getBlocks();
    }

    // Add file blocks to a given data node
    public void addBlock(String address, String fileName, long indexOfBlock){
        this.mapOfAddressAndNodeInfo.get(address).addBlock(fileName, indexOfBlock);
    }

    // Available or not
    public boolean isAvailable(String address){
        return this.mapOfAddressAndNodeInfo.get(address).isAvailable();
    }

    // Obtain all the data node
    public List<String> getAllDataNode(){
        return new ArrayList<>(this.mapOfAddressAndNodeInfo.keySet());
    }

    // Remove the given block
    public void removeBlock(String fileName, long indexOfBlock){
        for (String dataNode: this.mapOfAddressAndNodeInfo.keySet()){
            this.mapOfAddressAndNodeInfo.get(dataNode).removeBlock(fileName, indexOfBlock);
        }
    }

    public boolean isContained(String address){
        return this.mapOfAddressAndNodeInfo.containsKey(address);
    }

    // Transfer the block
    public void transferBlock(String oldDataNode, String newDataNode, String fileName, long indexOfBlock){
        this.mapOfAddressAndNodeInfo.get(newDataNode).addBlock(fileName, indexOfBlock);
        this.mapOfAddressAndNodeInfo.get(oldDataNode).removeBlock(fileName, indexOfBlock);
    }

    public void showDataNodeContent(){
        System.out.println("DataNode Manager: ");

        for (Map.Entry<String, DataNodeInfo> entry: this.mapOfAddressAndNodeInfo.entrySet()) {
            System.out.println("DataNode Addr. " + entry.getKey());
            for (Pair<String, Long> block: entry.getValue().getBlocks()){
                System.out.println("\t : "+block.getKey()+"#"+block.getValue());
            }
        }
    }
}
