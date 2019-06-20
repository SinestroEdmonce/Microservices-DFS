package com.joey.sa.mdfs.datanode.Database;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;


/**
 * @projectName Microservices-DFS
 * @fileName DatabaseService
 * @auther Qiaoyi Yin
 * @time 2019-06-20 12:36
 * @function Database Service, used as an interface for whole file system storage services
 */
public interface DatabaseService {

    void init();

    void save(MultipartFile file);

    Stream<Path> loadAll();

    Path load(String fileName);

    Resource loadAsResource(String fileName);

    void deleteAll();

    void delete(String fileName);
}
