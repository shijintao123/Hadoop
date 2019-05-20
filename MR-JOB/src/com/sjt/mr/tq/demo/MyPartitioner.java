package com.sjt.mr.tq.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends  Partitioner<MyTQ,IntWritable>{

	@Override
	public int getPartition(MyTQ key, IntWritable value, int numPartitions) {
		//根据key的年月去分区
		String factor=key.getYear()+"-"+key.getMonth();
		return factor.hashCode()%numPartitions;
	}

}
