package com.sjt.mr.tq.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends  Partitioner<MyTQ,IntWritable>{

	@Override
	public int getPartition(MyTQ key, IntWritable value, int numPartitions) {
		//����key������ȥ����
		String factor=key.getYear()+"-"+key.getMonth();
		return factor.hashCode()%numPartitions;
	}

}
