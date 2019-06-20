package com.joey.sa.mdfs.namenode.FileSystem;

import com.joey.sa.mdfs.namenode.Error.ErrorPrinter;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @projectName Microservices-DFS
 * @fileName FileManager
 * @auther Qiaoyi Yin
 * @time 2019-06-19 11:28
 * @function File Manager, used to store the structure of files and all files from users
 */
public class FileManager {

    // A tree structure for file storage
    private class FileTreeNode {
        // Directory or not
        boolean isDirectory;

        // File name or directory name
        String name;

        // File's storage attributes.
        long blockNum;
        long fileSize;

        List<FileTreeNode> children = new LinkedList<>();

        public FileTreeNode(boolean isDir, String name, long blockNum, long fileSize){
            this.isDirectory = isDir;
            this.name = name;
            this.blockNum = blockNum;
            this.fileSize = fileSize;
        }
    }

    FileTreeNode root = new FileTreeNode(true, "file-system-root", 0, 0);

    // Search for the given file name by going through the whole file tree
    private Pair<FileTreeNode, String> walkFileTree(String filePath){
        FileTreeNode node = this.root;
        String[] paths = filePath.split("/");
        int pathLength = paths.length;

        // Is a directory
        for (int idx=0; idx<pathLength-1; ++idx){
            FileTreeNode next = null;

            // Search for the given file name
            for (FileTreeNode child: node.children){
                if (child.isDirectory && child.name.equals(paths[idx])){
                    next = child;
                    break;
                }
            }

            if (next==null){
                // Output error information
                new ErrorPrinter(1, filePath).printError();
                return null;
            }

            node = next;
        }

        String fileName = paths[pathLength-1];
        return new Pair<>(node, fileName);
    }

    // Add files to the file tree
    public void addFile(String filePath, long blockNum, long fileSize){
        FileTreeNode node = this.root;
        String[] paths = filePath.split("/");
        int pathLength = paths.length;

        // Is a directory
        for (int idx=0; idx<pathLength-1; ++idx){
            FileTreeNode next = null;

            // Search for the correct storage location
            for (FileTreeNode child: node.children){
                if (child.isDirectory && child.name.equals(paths[idx])){
                    next = child;
                    break;
                }
            }

            if (next==null){
                FileTreeNode newPathNode = new FileTreeNode(true, paths[idx], 0, 0);
                node.children.add(newPathNode);
                next = newPathNode;
            }

            node = next;
        }

        String fileName = paths[pathLength-1];
        node.children.add(new FileTreeNode(false, fileName, blockNum, fileSize));
    }

    // Obtain the number of blocks
    public long getBlockNum(String filePath){
        Pair<FileTreeNode, String> pairOfNodeAndFile = this.walkFileTree(filePath);

        // Path not exists
        if (pairOfNodeAndFile == null){
            return 0;
        }

        FileTreeNode node = pairOfNodeAndFile.getKey();
        String fileName = pairOfNodeAndFile.getValue();

        for (FileTreeNode child: node.children){
            if (!child.isDirectory && child.name.equals(fileName)){
                return child.blockNum;
            }
        }

        // File not exists
        new ErrorPrinter(2, fileName);
        return 0;
    }

    // Delete File
    public void deleteFile(String filePath){
        Pair<FileTreeNode, String> pairOfNodeAndFile = this.walkFileTree(filePath);

        // Path not exists
        if (pairOfNodeAndFile == null){
            return;
        }

        FileTreeNode node = pairOfNodeAndFile.getKey();
        String fileName = pairOfNodeAndFile.getValue();

        for (FileTreeNode child: node.children){
            if (!child.isDirectory && child.name.equals(fileName)){
                node.children.remove(child);
                return;
            }
        }

        // File not exists
        new ErrorPrinter(2, fileName);
        return;
    }

    // Obtain the size of a given file
    public long getFileSize(String filePath){
        Pair<FileTreeNode, String> pairOfNodeAndFile = this.walkFileTree(filePath);

        // Path not exists
        if (pairOfNodeAndFile == null){
            return 0;
        }

        FileTreeNode node = pairOfNodeAndFile.getKey();
        String fileName = pairOfNodeAndFile.getValue();

        for (FileTreeNode child: node.children){
            if (!child.isDirectory && child.name.equals(fileName)){
                return child.fileSize;
            }
        }

        // File not exists
        new ErrorPrinter(2, fileName);
        return 0;
    }

    // File exist or not
    public boolean isContained(String filePath){
        Pair<FileTreeNode, String> pairOfNodeAndFile = this.walkFileTree(filePath);

        // Path not exists
        if (pairOfNodeAndFile == null){
            return false;
        }

        FileTreeNode node = pairOfNodeAndFile.getKey();
        String fileName = pairOfNodeAndFile.getValue();

        for (FileTreeNode child: node.children){
            if (!child.isDirectory && child.name.equals(fileName)){
                return true;
            }
        }

        // File not exists
        return false;
    }

    // Add node to the map
    private void addNode2Map(Map<String, Long> map, String filePath, FileTreeNode node){
        if (!node.isDirectory){
            map.put(filePath + node.name, node.fileSize);
        }
        else {
            filePath += node.name + "/";
            for (FileTreeNode child: node.children){
                this.addNode2Map(map, filePath, child);
            }
        }
    }

    // Obtain the file list
    public Map<String, Long> list(){
        Map<String, Long> mapOfFileAndSize = new HashMap<>();

        FileTreeNode node = root;
        for (FileTreeNode child: node.children){
            this.addNode2Map(mapOfFileAndSize, "", child);
        }
        return mapOfFileAndSize;
    }

}
