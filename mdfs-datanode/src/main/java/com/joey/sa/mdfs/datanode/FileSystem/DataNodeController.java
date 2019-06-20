package com.joey.sa.mdfs.datanode.FileSystem;

import java.io.File;
import java.io.IOException;

import java.lang.reflect.Array;

import java.nio.file.Path;

import java.util.*;
import java.util.stream.Collectors;

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

}
