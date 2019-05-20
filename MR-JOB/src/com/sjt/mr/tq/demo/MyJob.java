package com.sjt.mr.tq.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//加载配置文件
		Configuration conf =new Configuration(true);
		//创建Job对象
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyJob.class);
		job.setJobName("myjob");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path path = new Path(args[1]);
		if(path.getFileSystem(conf).exists(path)){
			//如果存在文件则递归删除
			path.getFileSystem(conf).delete(path,true);
		}
		FileOutputFormat.setOutputPath(job, path);
		//====map部分
		//map的输出部分的key
		job.setMapOutputKeyClass(MyTQ.class);
		//map的输出部分的value
		job.setMapOutputValueClass(IntWritable.class);
		//mapper
		job.setMapperClass(MyMapper.class);
		//分区器
		job.setPartitionerClass(MyPartitioner.class);
		//分组比较器
		job.setGroupingComparatorClass(MyGroupComparator.class);
		//reduce
		job.setReducerClass(MyReduce.class);
		//提交作业
		job.waitForCompletion(true);
	}
}
