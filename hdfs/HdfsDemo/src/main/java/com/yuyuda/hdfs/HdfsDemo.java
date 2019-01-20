package com.yuyuda.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class HdfsDemo {
    FileSystem fs;
    Configuration conf;

    @Before
    public void begin() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        fs = FileSystem.get(conf);
    }

    @After
    public void end() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/yyd");
        boolean res = fs.mkdirs(path);
        if (res) {
            System.out.println("文件夹创建成功！");
        } else  {
            System.out.println("文件夹创建失败！");
        }
    }

    @Test
    public void upload() throws IOException {
        Path path = new Path("/yyd/test");
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        FileUtils.copyFile(new File("D:\\java\\HdfsDemo\\pom.xml"), fsDataOutputStream);
    }

    @Test
    public void upload2() throws IOException {
        Path path = new Path("/yyd/seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
        File file = new File("D:\\java\\HdfsDemo\\data\\seq");
        for (File f : file.listFiles()) {
            writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
        }
    }

    @Test
    public void list() throws IOException {
        Path path = new Path("/yyd");
        FileStatus[] fss = fs.listStatus(path);
        for (FileStatus fs : fss) {
            System.out.println(fs.getAccessTime() + "-" + fs.getBlockSize() + "-" + fs.getLen()
                    + "-" + fs.getModificationTime() + "-" + fs.getReplication()
                    + "-" + fs.getGroup() + "-" + fs.getOwner() + "-" + fs.getPath()
                    + "-" + fs.getPermission() + "-" + fs.getClass()
            );
        }
    }

    @Test
    public void download() throws IOException {
        Path srcPath = new Path("/yyd/test");
        Path dstPath = new Path("D:\\java\\HdfsDemo\\data\\download");
        fs.copyToLocalFile(srcPath, dstPath);
    }

    @Test
    public void downloadSeq() throws IOException {
        Path path = new Path("/yyd/seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        Text val = new Text();

        while (reader.next(key, val)) {
            System.out.println("filename: " + key);
            System.out.println("filecontent: " + val);
            System.out.println("---------------------------");
        }
    }
}
