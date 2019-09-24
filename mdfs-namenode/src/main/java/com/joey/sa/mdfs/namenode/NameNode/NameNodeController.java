package com.joey.sa.mdfs.namenode.NameNode;

import com.joey.sa.mdfs.namenode.DataNode.DataNodeManager;
import com.joey.sa.mdfs.namenode.Error.ErrorPrinter;
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
    @Value(value="${load-balance.virtual-node-num}")
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

    // Obtain file name and block number
    private Pair<String, Long> parseBlockName(String blockName) {
        int splitIndex = blockName.indexOf('_');
        String fileName = blockName.substring(splitIndex+1, blockName.length());
        Long blockIndex = Long.parseLong(blockName.substring(0, splitIndex));

        return new Pair<>(fileName, blockIndex);
    }

    // Transfer blocks to a new node
    private boolean transferBlocks2NewNode(String newDataNode) {
        try {
            Map<String, String> mapOfFileAndDataNode = loadBalanceManager.transferFiles2NewDataNode(newDataNode);
            for (Map.Entry<String, String> entry: mapOfFileAndDataNode.entrySet()) {
                String oldDataNode = entry.getValue();
                String blockName = entry.getKey();
                File file = this.getFileBlock(oldDataNode, blockName);
                this.saveBlock(newDataNode, file);
                this.deleteFileBlock(oldDataNode, blockName);

                Pair<String, Long> fb = this.parseBlockName(blockName);
                this.dataNodeManager.transferBlock(oldDataNode, newDataNode, fb.getKey(), fb.getValue());
                this.blockManager.transferBlock(oldDataNode, newDataNode, fb.getKey(), fb.getValue());
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Transfer blocks from the offline data node to other nodes
    private boolean transferBlocks4OfflineNode(String offlineDataNode) {
        try {
            Map<String, String> mapOfFileAndDataNode = loadBalanceManager.transferFiles4OfflineDataNode(offlineDataNode);
            List<Pair<String, Long>> blocks = this.dataNodeManager.getBlocks(offlineDataNode);

            for (Pair<String, Long> block: blocks) {
                // Find data nodes to store the blocks from the offline node
                String fileName = block.getKey();
                long blockIndex = block.getValue();
                List<String> dataNodeList = this.blockManager.getDataNode(fileName, blockIndex);

                // don't need the removed data node
                // don't need the nodes that possess the file block
                dataNodeList.removeIf(x->x.equals(offlineDataNode) || this.dataNodeManager.getBlocks(x).contains(fileName));

                if (dataNodeList.size()==0){
                    new ErrorPrinter(8, fileName+"#"+blockIndex);
                }
                else{
                    String dataNodeAddr = dataNodeList.get(0);
                    String blockName = this.getFileBlockName(fileName, blockIndex);
                    File fileBlock = this.getFileBlock(dataNodeAddr, blockName);

                    // Search for a new node to store the file block
                    String dataNode4Storage = mapOfFileAndDataNode.get(blockName);
                    if (dataNode4Storage==null){
                        new ErrorPrinter(9, fileName+"#"+blockIndex);
                    }
                    else{
                        boolean response = this.saveBlock(dataNode4Storage, fileBlock);
                        System.out.println(String.format("FILE (%s) ", fileName.replace("_", "/"))+
                                "Block #"+blockIndex+
                                " -> "+dataNode4Storage+(response? ": Success": ": Fail"));

                        if (response){
                            this.dataNodeManager.addBlock(dataNode4Storage, fileName, blockIndex);
                            this.blockManager.addBlock(fileName, blockIndex, dataNode4Storage);
                        }
                        fileBlock.delete();
                    }
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Write a block
    private File writeBlock(String fileName, byte[] blockByteArray, long blockIndex, int blockSize) {
        try {
            String blockFileName = BLOCKS_DIR + this.getFileBlockName(fileName, blockIndex);
            File slicedFile = new File(blockFileName);
            FileOutputStream fos = new FileOutputStream(slicedFile);
            fos.write(blockByteArray, 0, blockSize);
            fos.close();
            return slicedFile;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Store file blocks by sending the file block to the given data node' address
    private boolean saveBlock(String dataNode, File file) {
        // Prepare the parameter
        FileSystemResource resource = new FileSystemResource(file);
        MultiValueMap<String, Object> parameters = new LinkedMultiValueMap<>();
        parameters.add("file", resource);

        // Send post request
        RestTemplate rest = new RestTemplate();
        String response = rest.postForObject(dataNode, parameters, String.class);

        return response.equals("Success");
    }

    // Obtain the data node for storage
    private List<String> getDataNode4Storage(File file) {
        return loadBalanceManager.getDataNode4Storage(file, this.REPLICA_NUM);
    }

    // Write a file
    private File writeFile(String fileName, byte[] blockByteArray, int length) {
        try {
            File file = new File(BLOCKS_DIR + fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(blockByteArray, 0, length);
            fileOutputStream.close();
            return file;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Get file blocks
    private File getFileBlock(String dataNode, String fileName) {
        try {
            // Download file into a input stream
            String resource = dataNode + "files/" + fileName;

            UrlResource urlResource = new UrlResource(new URL(resource));
            InputStream inputStream = urlResource.getInputStream();


            // Write into a byte array
            byte[] bytes = new byte[this.BLOCK_SIZE];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int num, blockSize = 0;
            while ((num=inputStream.read(bytes))!= -1) {
                out.write(bytes,0, num);
                blockSize += num;
            }

            bytes = out.toByteArray();
            return writeFile(fileName, bytes, blockSize);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    // Delete all blocks of the given file by sending a message to a data node to delete blocks
    private void deleteFileBlock(String dataNodeURL, String fileName) {
        String block = dataNodeURL + "files/" + fileName;

        RestTemplate rest = new RestTemplate();
        rest.delete(block);
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

                        if (fileBlock!=null) {
                            FileChannel inputChannel = new FileInputStream(fileBlock).getChannel();
                            inputChannel.transferTo(0, fileBlock.length(), outputChannel);
                            inputChannel.close();
                            break;
                        }
                        else{
                            new ErrorPrinter(1, filePath);
                            return ResponseEntity.notFound().build();
                        }
                    }
                }
            }

            outputChannel.close();
            Resource resource = new UrlResource(file2Download.toURI());

            return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename="+"\""+resource.getFilename()+"\"").body(resource);
        }
        catch (Exception e){
            e.printStackTrace();
            return ResponseEntity.notFound().build();
        }
    }

    @PutMapping("/**")
    public String handleFileUpload(@RequestParam("file") MultipartFile file){
        try{
            String path = request.getRequestURI().replaceFirst("/", "");
            String filePath = path+file.getOriginalFilename();
            String fileName = path.replace("/", "_")+file.getOriginalFilename();

            if (this.fileManager.isContained(filePath)){
                new ErrorPrinter(4, fileName);
                return "Fail. File already exists!";
            }

            // Split into blocks
            long byteNum = file.getSize();
            long blockNum = byteNum/this.BLOCK_SIZE+((byteNum%this.BLOCK_SIZE)==0? 0: 1);

            FileInputStream fileInputStream = (FileInputStream) file.getInputStream();
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);

            for (int indexOfBlock=0; indexOfBlock<blockNum; ++indexOfBlock){
                byte[] blockByteArray = new byte[this.BLOCK_SIZE];
                int blockSize = bufferedInputStream.read(blockByteArray);
                File block = writeBlock(fileName, blockByteArray, indexOfBlock, blockSize);

                List<String> dataNode4Storage = this.getDataNode4Storage(block);

                for (String dataNode: dataNode4Storage){
                    boolean response = saveBlock(dataNode, block);
                    System.out.println("Block #"+indexOfBlock+" -> "+dataNode+": "+
                            (response? "Success": "Fail"));
                    this.dataNodeManager.addBlock(dataNode, fileName, indexOfBlock);
                    this.blockManager.addBlock(fileName, indexOfBlock, dataNode);
                }

                boolean __ = block.delete();
            }
            this.fileManager.addFile(filePath, blockNum, byteNum);
            return "Success";
        }
        catch (Exception e){
            e.printStackTrace();
            return "Fail";
        }
    }

    @DeleteMapping("/**")
    @ResponseBody
    public String handleFileDelete(){
        String filePath = request.getRequestURI().replaceFirst("/", "");
        String fileName = filePath.replace("/", "_");

        if (!this.fileManager.isContained(filePath)){
            new ErrorPrinter(2, filePath);
            return "Fail. File does not exist!";
        }

        long blockNum = this.fileManager.getBlockNum(filePath);
        for (int indexOfBlock=0; indexOfBlock<blockNum; ++indexOfBlock){
            List<String> dataNodeAddr = this.blockManager.getDataNode(fileName, indexOfBlock);

            for (String address: dataNodeAddr){
                String blockName = this.getFileBlockName(fileName, indexOfBlock);
                this.deleteFileBlock(address, blockName);

                System.out.println(String.format("FILE (%s) ", filePath)+
                        "Block #"+indexOfBlock+
                        " .:"+address+": Deleted");
            }

            this.dataNodeManager.removeBlock(fileName, indexOfBlock);
            this.loadBalanceManager.removeFile(this.getFileBlockName(fileName, indexOfBlock));
            this.blockManager.removeBlock(fileName, indexOfBlock);
        }
        this.fileManager.deleteFile(filePath);
        return "Success";
    }

    private void showManagers() {
        if (this.TEST_MODE) {
            this.dataNodeManager.showDataNodeContent();
            this.blockManager.showBlocksDistribution();
            this.loadBalanceManager.showFilesOnDataNode();
        }
    }

    // A Data node is offline
    @EventListener
    public void listen(EurekaInstanceCanceledEvent event) throws IOException{
        synchronized (this){
            String dataNodeAddr = "http://"+event.getServerId()+"/";
            new ErrorPrinter(5, dataNodeAddr);

            if (this.dataNodeManager.isContained(dataNodeAddr)){
                this.transferBlocks4OfflineNode(dataNodeAddr);
                this.dataNodeManager.removeDataNode(dataNodeAddr);
                this.blockManager.removeDataNode(dataNodeAddr);
                this.loadBalanceManager.removeDataNode(dataNodeAddr);
                new ErrorPrinter(7, dataNodeAddr);
            }
            else{
                new ErrorPrinter(6, dataNodeAddr);
            }
        }
        this.showManagers();
    }

    // A new data node is registered
    @EventListener
    public void listen(EurekaInstanceRegisteredEvent event){
        synchronized (this){
            InstanceInfo instanceInfo = event.getInstanceInfo();
            String newDataNodeAddr = instanceInfo.getHomePageUrl();
            new ErrorPrinter(10, newDataNodeAddr);

            if (this.dataNodeManager.isContained(newDataNodeAddr)){
                new ErrorPrinter(11, newDataNodeAddr);
            }
            else{
                this.dataNodeManager.addDataNode(newDataNodeAddr);
                this.loadBalanceManager.addDataNode(newDataNodeAddr);
                this.transferBlocks2NewNode(newDataNodeAddr);
                new ErrorPrinter(12, newDataNodeAddr);
            }
            this.showManagers();
        }
    }

    // Delete temp files
    private void deleteTempFiles(){
        try{
            FileSystemUtils.deleteRecursively(Paths.get(BLOCKS_DIR).toFile());
            Files.createDirectories(Paths.get(BLOCKS_DIR));
            FileSystemUtils.deleteRecursively(Paths.get(DOWNLOAD_DIR).toFile());
            Files.createDirectories(Paths.get(DOWNLOAD_DIR));
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    // One pulse from a data node
    @EventListener
    public void listen(EurekaInstanceRenewedEvent event){
        new ErrorPrinter(13, event.getServerId()+"#"+event.getAppName());
    }

    // Ready to register data nodes
    @EventListener
    public void listen(EurekaRegistryAvailableEvent event){
        new ErrorPrinter(14, "");
    }

    // Start Eureka server
    @EventListener
    public void listen(EurekaServerStartedEvent event){
        new ErrorPrinter(15, "");

        this.deleteTempFiles();
        boolean __ = new File(BLOCKS_DIR).mkdirs();
        __ = new File(DOWNLOAD_DIR).mkdirs();

        this.loadBalanceManager.setNumVirtualNodes(this.VIRTUAL_NODE_NUMBER);
    }
}
