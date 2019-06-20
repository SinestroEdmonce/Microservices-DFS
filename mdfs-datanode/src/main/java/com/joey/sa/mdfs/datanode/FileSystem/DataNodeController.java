package com.joey.sa.mdfs.datanode.FileSystem;

import java.io.File;
import java.io.IOException;

import java.lang.reflect.Array;

import java.nio.file.Path;

import java.util.*;
import java.util.stream.Collectors;

import com.joey.sa.mdfs.datanode.Database.DatabaseFileNotFoundException;
import com.joey.sa.mdfs.datanode.Database.DatabaseService;
import javafx.beans.binding.ObjectExpression;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.MvcUriComponentsBuilder;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;


/**
 * @projectName Microservices-DFS
 * @fileName DataNodeController
 * @auther Qiaoyi Yin
 * @time 2019-06-20 12:13
 * @function Data Node Controller, used to handle requests from the NameNode
 */
@RestController
public class DataNodeController {

    private Map<String, BlockInfo> mapOfFileAndBlock = new HashMap<>();
    private final DatabaseService databaseService;


    @Autowired
    public DataNodeController(DatabaseService databaseService){
        this.databaseService = databaseService;
    }

    @GetMapping("/")
    public Map<String, BlockInfo> listUploadedFiles() throws IOException{
        return this.mapOfFileAndBlock;
    }

    @GetMapping("/files/{filename:.+}")
    @ResponseBody
    public ResponseEntity<Resource> handleFileDownload(@PathVariable String fileName){

        Resource file = this.databaseService.loadAsResource(fileName);
        return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION,
                "attachment;filename=\"" + file.getFilename() + "\"").body(file);
    }

    @DeleteMapping("/files/{filename:.+}")
    @ResponseBody
    public String deleteFile(@PathVariable String filename){
        this.databaseService.delete(filename);
        this.mapOfFileAndBlock.remove(filename);
        return "Success";
    }

    @DeleteMapping("/allFiles")
    @ResponseBody
    public String deleteAll(){
        this.databaseService.deleteAll();
        this.mapOfFileAndBlock.clear();
        return "Success";
    }

    @PostMapping("/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file){
        String fileName = file.getOriginalFilename();
        if (this.mapOfFileAndBlock.keySet().contains(fileName)){
            return "Fail";
        }

        // Save files on the data node
        this.databaseService.save(file);

        // record file/block information
        long fileSize = file.getSize();
        BlockInfo blockInfo = new BlockInfo(fileName, fileSize);
        this.mapOfFileAndBlock.put(fileName, blockInfo);

        return "Success";
    }

    @ExceptionHandler(DatabaseFileNotFoundException.class)
    public ResponseEntity<?> handleStorageFileNotFound(DatabaseFileNotFoundException e){
        return ResponseEntity.notFound().build();
    }

}

