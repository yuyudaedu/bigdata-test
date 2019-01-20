package com.yuyuda.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCJob {

    public static void main(String[] args) throws IOException {
        //System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://node1:8020");
        //conf.set("yarn.resourcemanager.hostname", "node4");
        //conf.set("mapred.jar", "D:\\tools\\localProjectPackageJAR\\wc.jar");

        Job job = Job.getInstance(conf);

        //指定程序入口
        job.setJarByClass(WCJob.class);

        //指定Mapper
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定Reducer
        job.setReducerClass(WCReducer.class);
        FileInputFormat.addInputPath(job, new Path("/wc/input/wc"));
        Path outPath = new Path("/wc/out");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //提交
        try {
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("Job success! ");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
