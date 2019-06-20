package com.joey.sa.mdfs.namenode.FileSystem;

import com.joey.sa.mdfs.namenode.Error.ErrorPrinter;

import java.io.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;


/**
 * @projectName Microservices-DFS
 * @fileName LoadBalanceManager
 * @auther Qiaoyi Yin
 * @time 2019-06-19 11:28
 * @function Load Balance Manager, used to keep load balance,
 *           and transfer file blocks from one data node to another when necessary
 */
public class LoadBalanceManager {

    // Class that is used to map file names to hash code
    private class HashNode{

        int hashCode;
        String address;
        Map<String, Integer> mapOfFileAndHash = new HashMap<>();

        HashNode(int hashCode, String address){
            this.hashCode = hashCode;
            this.address = address;
        }
    }

    // Class that is used to record transfer of files
    private class TransferRecord{

        int newIndexOfNode;
        int oldInderOfNode;
        int fileHashCode;
        String fileName;

        public TransferRecord(int oldIndex, int newIndex, String fileName, int hashCode){
            this.newIndexOfNode = newIndex;
            this.oldInderOfNode = oldIndex;
            this.fileHashCode = hashCode;
            this.fileName = fileName;
        }
    }

    private Set<String> dataNodeSet = new HashSet<>();
    private List<HashNode> hashNodeList = new LinkedList<>();
    private Map<String, List<String>> mapOfFileAndDataNode = new HashMap<>();

    private int numVirtualNodes;

    public LoadBalanceManager(int numVirtualNodes){
        this.numVirtualNodes = numVirtualNodes;
    }

    // Hash operation for files and data node, including virtual nodes
    private int hashCode(String source){
        // FNV1_32_HASH Algorithm
        int tmp = 16777619;
        int hash = (int)2166136261L;

        for (int idx=0; idx<source.length(); ++idx){
            hash = (hash^source.charAt(idx))*tmp;
        }
        hash = hash+(hash<<13);
        hash = hash^(hash>>7);
        hash = hash+(hash<<3);
        hash = hash^(hash>>17);
        hash = hash+(hash<<5);

        return hash;
    }

    // Hash file into code
    private int hashCode(File file){
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String lineContent = null;
            StringBuilder fileContent = new StringBuilder("");

            while((lineContent=bufferedReader.readLine())!=null){
                fileContent.append(lineContent);
            }

            return this.hashCode(fileContent.toString());
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return file.hashCode();
    }

    // Update the file map
    private void updateFileMap() {
        this.mapOfFileAndDataNode.clear();

        for (HashNode node : this.hashNodeList) {
            for (String fileName : node.mapOfFileAndHash.keySet()) {
                if (!this.mapOfFileAndDataNode.keySet().contains(fileName)) {
                    this.mapOfFileAndDataNode.put(fileName, new LinkedList<>());
                }
                this.mapOfFileAndDataNode.get(fileName).add(node.address);
            }
        }
    }

    // Synchronized add data node
    private synchronized void addDataNode(int hashCode, String address){
        int hashCodeIndex = 0;

        // Search for the index for hashCode to insert
        // Obey the ascending order
        for (; hashCodeIndex<this.hashNodeList.size(); ++hashCodeIndex){
            HashNode node = this.hashNodeList.get(hashCodeIndex);
            if (node.hashCode == hashCode){
                hashCode += 1;
            }
            if (node.hashCode > hashCode){
                break;
            }
        }

        if (hashCodeIndex == this.hashNodeList.size()) {
            // Insert data node to the tail
            this.hashNodeList.add(new HashNode(hashCode, address));
        } else {
            this.hashNodeList.add(hashCodeIndex, new HashNode(hashCode, address));
        }

        this.updateFileMap();
    }

    public void addDataNode(String address){
        this.dataNodeSet.add(address);
        for (int visualNodeIndex=0; visualNodeIndex<=this.numVirtualNodes; ++visualNodeIndex){
            this.addDataNode(this.hashCode(address+"#"+visualNodeIndex), address);
        }

        this.updateFileMap();
    }

    public void removeDataNode(String address){
        this.dataNodeSet.remove(address);
        this.hashNodeList.removeIf(x -> x.address.equals(address));

        this.updateFileMap();
    }

    public void removeFile(String fileName){
        for (HashNode node: this.hashNodeList){
            node.mapOfFileAndHash.remove(fileName);
        }

        this.updateFileMap();
    }

    public void setNumVirtualNodes(int numVirtualNodes){
        this.numVirtualNodes = numVirtualNodes;
    }

    // Obtain the correct storage position for the given file
    public List<String> getDataNode4Storage(File file, int replicas){
        String fileName = file.getName();
        List<String> dataNodeList4Storage = new LinkedList<>();

        // Empty or not (DataNodes exist or not)
        if (this.dataNodeSet.isEmpty()){
            return dataNodeList4Storage;
        }

        // Find a hash node for file hashcode to insert
        int fileHashCode = this.hashCode(file);
        String dataNodeAddr = null;
        int indexOfHashNode = 0;
        for (HashNode node: this.hashNodeList){
            if (node.hashCode>=fileHashCode){
                dataNodeAddr = node.address;
                indexOfHashNode = this.hashNodeList.indexOf(node);
                node.mapOfFileAndHash.put(fileName, fileHashCode);
                break;
            }
        }

        // If (n-1) data nodes have no place for the current file, insert it into the first data node
        if (dataNodeAddr==null){
            dataNodeAddr = this.hashNodeList.get(0).address;
            this.hashNodeList.get(0).mapOfFileAndHash.put(fileName, fileHashCode);
        }
        dataNodeList4Storage.add(dataNodeAddr);

        // Allocate data nodes for replica
        // Simply find the next (replicas - 1) nodes and set hashcode to half of the two nodes
        indexOfHashNode = (indexOfHashNode+1)%this.hashNodeList.size();
        for (int indexOfReplica=0; indexOfReplica<replicas-1; ++indexOfReplica){
            if ((indexOfReplica+2)>this.dataNodeSet.size()){
                new ErrorPrinter(3,
                        String.format("Current DataNodes: %d, RequiredReplicas: %d", this.dataNodeSet.size(), replicas));
                break;
            }

            // Next storage location, in order to avoid two or more replicas are stored in the same data node
            while(dataNodeList4Storage.contains(this.hashNodeList.get(indexOfHashNode).address)){
                indexOfHashNode = (indexOfHashNode+1)%this.hashNodeList.size();
            }

            dataNodeList4Storage.add(this.hashNodeList.get(indexOfHashNode).address);

            // Set hashcode to half of the two nodes
            int code = this.hashNodeList.get(indexOfHashNode).hashCode;
            int preCode = (indexOfHashNode-1)>=0?
                    this.hashNodeList.get(indexOfHashNode-1).hashCode:
                    this.hashNodeList.get(this.hashNodeList.size()-1).hashCode;

            // If code is the first hash node and previous one is the last hash node, make hashCode equals to MAX_VALUE
            int hashCode = (code-preCode)>=0?(preCode+(code-preCode)/2): Integer.MAX_VALUE;
            this.hashNodeList.get(indexOfHashNode).mapOfFileAndHash.put(fileName, hashCode);
        }

        this.updateFileMap();
        return dataNodeList4Storage;
    }

    // Record transfer information of files on the data node that will be removed later
    public Map<String, String> transferFiles4OfflineDataNode(String address){
        // Check status of files' storage locations before the re-allocation
        this.showFilesOnDataNode();
        Map<String, String> mapOfFileAndAddr = new HashMap<>();

        for (HashNode node: this.hashNodeList){
            int indexOfHashNode = this.hashNodeList.indexOf(node);

            // If there exists a hash node(virtual or real) that has the same address,
            // owned by the data node which will be removed later,
            // we should transfer all files on that data node to others
            if (node.address.equals(address)){
                for (String fileName: node.mapOfFileAndHash.keySet()){
                    int fileHashCode = node.mapOfFileAndHash.get(fileName);

                    // Find an appropriate(first) node for files to be stored on
                    // Obey: 1. No other replicas of the given file are stored on the node
                    //       2. The node should not belong to the data node that will be removed later
                    for (int idx=indexOfHashNode+1; idx!=indexOfHashNode; idx=(idx+1)%this.hashNodeList.size()){
                        if (!this.mapOfFileAndDataNode.get(fileName).contains(this.hashNodeList.get(idx).address) &&
                                !this.hashNodeList.get(idx).address.equals(address)){

                            mapOfFileAndAddr.put(fileName, this.hashNodeList.get(idx).address);
                            this.hashNodeList.get(idx).mapOfFileAndHash.put(fileName, fileHashCode);
                            break;
                        }
                    }
                }
            }
        }

        this.updateFileMap();
        // Check results after re-allocation
        this.showFilesOnDataNode();

        return mapOfFileAndAddr;
    }

    // Allocate some file blocks from nodes nearby to the new node
    public Map<String, String> transferFiles2NewDataNode(String newAddress){
        List<TransferRecord> transferRecordList = new LinkedList<>();
        // Record the file list on the new data node
        HashMap<String, String> mapOfFileAndAddr = new HashMap<>();

        for (int indexOfNode=0; indexOfNode<this.hashNodeList.size(); ++indexOfNode){
            HashNode currentNode = this.hashNodeList.get(indexOfNode);

            if (currentNode.address.equals(newAddress)) {
                int nextIndex = (indexOfNode + 1) % this.hashNodeList.size();
                if (this.hashNodeList.get(nextIndex).address.equals(newAddress)) {
                    continue;
                }


                for (Map.Entry<String, Integer> file2Hash:
                        this.hashNodeList.get(nextIndex).mapOfFileAndHash.entrySet()){

                    // Some file blocks can be allocated to the new node, according to the hash code
                    if (file2Hash.getValue() <= currentNode.hashCode){
                        // No replicas of the given file have been stored on the current node
                        if (!mapOfFileAndAddr.keySet().contains(file2Hash.getKey())){
                            mapOfFileAndAddr.put(file2Hash.getKey(), this.hashNodeList.get(nextIndex).address);
                            transferRecordList.add(
                                    new TransferRecord(nextIndex, indexOfNode, file2Hash.getKey(), file2Hash.getValue()));
                        }
                    }
                }

            }
        }

        // Transfer the files, according to the records
        for (TransferRecord record: transferRecordList){
            this.hashNodeList.get(record.oldInderOfNode).mapOfFileAndHash.remove(record.fileName);
            this.hashNodeList.get(record.newIndexOfNode).mapOfFileAndHash.put(record.fileName, record.fileHashCode);
        }

        this.updateFileMap();
        return mapOfFileAndAddr;
    }

    public void showFilesOnDataNode(){
        System.out.println("Load Balance Manager:");

        for (HashNode node: this.hashNodeList){
            if (!node.mapOfFileAndHash.isEmpty()){
                for (String fileName: node.mapOfFileAndHash.keySet()){
                    System.out.println("\t : " + fileName + "-> " + node.address);
                }
            }
        }
        System.out.println();
    }

}
