package com.qfk.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


import java.net.URI;


public class HdfsClient {
    @Test
    public void testMkdirs () throws Exception {
        Configuration  config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), config, "qfk");
        fs.mkdirs(new Path("/qfk/hadoop/mkdirTest2"));
        fs.close();
        System.out.print("over");
    }
}
