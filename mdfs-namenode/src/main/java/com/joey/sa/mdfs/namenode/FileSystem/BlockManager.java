package com.joey.sa.mdfs.namenode.FileSystem;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @projectName Microservices-DFS
 * @fileName BlockManager
 * @auther Qiaoyi Yin
 * @time 2019-06-19 11:28
 * @function Block Manager, used to store files' blocks
 */
public class BlockManager {

    private Map<Pair<String, Long>, List<String>> mapOfBlockAndDataNode = new HashMap<>();

    // Obtain the block information
    private Pair<String, Long> getBlockInfo(String filename, long indexOfBlock){
        return new Pair<>(filename, indexOfBlock);
    }

    // Obtain the data node information
    public List<String> getDataNode(String fileName, long indexOfBlock){
        return this.mapOfBlockAndDataNode.get(this.getBlockInfo(fileName, indexOfBlock));
    }

    // Add new blocks to manager
    public void addBlock(String fileName, long indexOfBlock, String dataNode){
        Pair<String, Long> blockInfo = this.getBlockInfo(fileName, indexOfBlock);
        List<String> dataNodeList = this.mapOfBlockAndDataNode.get(blockInfo);

        if (dataNodeList==null){
            dataNodeList = new LinkedList<>();
            dataNodeList.add(dataNode);
            this.mapOfBlockAndDataNode.put(blockInfo, dataNodeList);
        }
        else{
            dataNodeList.add(dataNode);
        }
    }

    // List all information
    public Map<Pair<String, Long>, List<String>> listAll(){
        return new HashMap<>(this.mapOfBlockAndDataNode);
    }

    // Remove data node
    public void removeDataNode(String dataNode){
        for (Map.Entry<Pair<String, Long>, List<String>> entry: this.mapOfBlockAndDataNode.entrySet()) {
            entry.getValue().removeIf(x -> x.equals(dataNode));
        }
    }

    // Remove block
    public void removeBlock(String fileName, long indexOfBlock){
        Pair<String, Long> blockInfo = this.getBlockInfo(fileName, indexOfBlock);
        this.mapOfBlockAndDataNode.remove(blockInfo);
    }

    // Transfer the block to another data node
    public void transferBlock(String oldDataNode, String newDataNode, String fileName, long indexOfBlock){
        Pair<String, Long> blockInfo = this.getBlockInfo(fileName, indexOfBlock);
        List<String> dataNodeList = this.mapOfBlockAndDataNode.get(blockInfo);

        dataNodeList.remove(oldDataNode);
        dataNodeList.add(newDataNode);
    }

    public void showBlocksDistribution(){
        System.out.println("Block Manager: ");

        for (Map.Entry<Pair<String, Long>, List<String>> entry: this.mapOfBlockAndDataNode.entrySet()) {
            System.out.println("\tBlock No. " + entry.getKey().getKey() + "#" + entry.getKey().getValue());
            for (String dataNode: entry.getValue()){
                System.out.println("\t\t - "+dataNode);
            }
        }
        System.out.println();
    }

}
