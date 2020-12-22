package com.qfk.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    @Test
    public void testMkdirs () throws Exception {
        Configuration  config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), config, "qfk");
        fs.mkdirs(new Path("/qfk/hadoop/mkdirTest2"));
        fs.close();
        System.out.print("over");
    }

    @Test
    public void testPut () throws Exception {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),config,"qfk");
        fs.copyFromLocalFile(new Path("D:\\hadoopExercise/text.txt"),new Path("/qfk/hadoop/input"));
        fs.close();
        System.out.print("over ...");
    }

    @Test
    public void testGet () throws URISyntaxException, IOException, InterruptedException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),config,"qfk");
        fs.copyToLocalFile(new Path("/qfk/hadoop/input/text.txt"),new Path("D:/"));
        fs.close();
        System.out.println("over!");
    }

    @Test
    public  void testDel () throws URISyntaxException, IOException, InterruptedException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"),config,"qfk");
        fs.delete(new Path(""),true);
        fs.close();
        System.out.println("over success");
    }
}
