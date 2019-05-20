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
		//���������ļ�
		Configuration conf =new Configuration(true);
		//����Job����
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyJob.class);
		job.setJobName("myjob");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path path = new Path(args[1]);
		if(path.getFileSystem(conf).exists(path)){
			//��������ļ���ݹ�ɾ��
			path.getFileSystem(conf).delete(path,true);
		}
		FileOutputFormat.setOutputPath(job, path);
		//====map����
		//map��������ֵ�key
		job.setMapOutputKeyClass(MyTQ.class);
		//map��������ֵ�value
		job.setMapOutputValueClass(IntWritable.class);
		//mapper
		job.setMapperClass(MyMapper.class);
		//������
		job.setPartitionerClass(MyPartitioner.class);
		//����Ƚ���
		job.setGroupingComparatorClass(MyGroupComparator.class);
		//reduce
		job.setReducerClass(MyReduce.class);
		//�ύ��ҵ
		job.waitForCompletion(true);
	}
}
