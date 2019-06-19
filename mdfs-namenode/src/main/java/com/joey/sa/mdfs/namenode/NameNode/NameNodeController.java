package com.joey.sa.mdfs.namenode.NameNode;

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



}
