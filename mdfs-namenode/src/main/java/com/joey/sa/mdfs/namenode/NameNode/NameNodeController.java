package com.joey.sa.mdfs.namenode.NameNode;

import com.joey.sa.mdfs.namenode.DataNode.DataNodeManager;
import com.joey.sa.mdfs.namenode.FileSystem.BlockManager;
import com.joey.sa.mdfs.namenode.FileSystem.FileManager;
import com.joey.sa.mdfs.namenode.FileSystem.LoadBalanceManager;
import com.netflix.appinfo.InstanceInfo;

import javafx.util.Pair;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.netflix.eureka.server.event.*;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.print.DocFlavor;
import javax.servlet.http.HttpServletRequest;

import java.io.*;

import java.net.URL;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @projectName Microservices-DFS
 * @fileName NameNodeController
 * @auther Qiaoyi Yin
 * @time 2019-06-19 11:00
 * @function REST Controller, used to response different requests from clients
 */
@RestController
public class NameNodeController {
    // Block configuration
    @Value(value="${block.default-replica-num}")
    public int REPLICA_NUM;

    @Value(value="${block.default-size}")
    public int BLOCK_SIZE;

    // Load balance configuration
    @Value(value="${load-balance.visual-node-num}")
    public int VIRTUAL_NODE_NUMBER;

    @Value(value="${test-mode}")
    public boolean TEST_MODE;

    @Autowired
    private HttpServletRequest request;

    private BlockManager blockManager = new BlockManager();
    private FileManager fileManager = new FileManager();
    private LoadBalanceManager loadBalanceManager = new LoadBalanceManager(this.VIRTUAL_NODE_NUMBER);
    private DataNodeManager dataNodeManager = new DataNodeManager();

    private static String BLOCKS_DIR = "tmp/blocks/";
    private static String DOWNLOAD_DIR = "tmp/download/";

    @GetMapping("/fileList")
    public Map<String, Long> listAllFiles(){
        return this.fileManager.list();
    }

    @GetMapping("/blocksOnNodes")
    public Map<String, List<String>> listBlocksOnNodes(){
        Map<Pair<String, Long>, List<String>> blockListOnNodes = this.blockManager.listAll();
        HashMap<String, List<String>> blocksList = new HashMap<>();

        for (Map.Entry<Pair<String, Long>, List<String>> entry: blockListOnNodes.entrySet()){
            StringBuilder blockName = new StringBuilder(entry.getKey().getKey()+"#"+entry.getKey().getValue());
            blocksList.put(blockName.toString(), entry.getValue());
        }

        return blocksList;
    }

    // Write a file
    private File writeFile(String fileName, byte[] blockByteArray, int length) {
        try {
            File file = new File(blockFileDir + fileName);
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(blockByteArray, 0, length);
            fos.close();
            return file;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Get file blocks
    private File getFileBlock(String dataNodeURL, String fileName) {
        try {
            // down load file into a input stream
            String resourceURL = dataNodeURL + "files/" + fileName;
            UrlResource urlResource = new UrlResource(new URL(resourceURL));
            InputStream inputStream = urlResource.getInputStream();

            // write into a byte array
            byte[] bytes = new byte[BLOCK_SIZE];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int n, blockSize = 0;
            while ( (n=inputStream.read(bytes)) != -1) {
                out.write(bytes,0,n);
                blockSize += n;
            }

            bytes = out.toByteArray();
            // write into a file
            File file = writeFile(fileName, bytes, blockSize);

            return file;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    // Obtain the file blocks
    private String getFileBlockName(String fileName, long blockIndex) {
        return blockIndex + "_" + fileName;
    }

    // Serve as a file downloading response
    @GetMapping("/**")
    @ResponseBody
    public ResponseEntity<Resource> handleFileDownload(){
        try {
            String filePath = request.getRequestURI().replaceFirst("/", "");
            String fileName = filePath.replace("/","_");

            // Download file and save it in a temp directory
            File file2Download = new File(DOWNLOAD_DIR+fileName);
            // Create a new file
            boolean __ = file2Download.createNewFile();
            // Use file channel to obtain a joint file
            FileChannel outputChannel = new FileOutputStream(file2Download).getChannel();

            // Get blocks of the file and join them together to form the original file
            long blockNum = this.fileManager.getBlockNum(filePath);
            for (int indexOfBlock=0; indexOfBlock<blockNum; ++indexOfBlock){
                List<String> dataNode = this.blockManager.getDataNode(fileName, indexOfBlock);
                String fileBlockName = this.getFileBlockName(fileName, indexOfBlock);

                for (String node: dataNode){
                    if (this.dataNodeManager.isAvailable(node)){
                        File fileBlock = this.getFileBlock(node, fileBlockName);
                        FileChannel inputChannel = new FileInputStream(fileBlock).getChannel();
                        inputChannel.transferTo(0,fileBlock.length(), outputChannel);
                        inputChannel.close();
                        break;
                    }
                }
            }

            outputChannel.close();
            Resource resource = new UrlResource(file2Download.toURI());

            return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment;filename="+"\""+resource.getFilename()+"\"").body(resource);
        }
        catch (Exception e){
            e.printStackTrace();
            return ResponseEntity.notFound().build();
        }
    }


}
