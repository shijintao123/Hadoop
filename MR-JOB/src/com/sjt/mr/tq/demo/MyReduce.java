package com.sjt.mr.tq.demo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<MyTQ, IntWritable,Text,IntWritable>{
	private Text key=new Text();
	private IntWritable value=new IntWritable();
	@Override
	protected void reduce(MyTQ mytq, Iterable<IntWritable> values,
			Reducer<MyTQ, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int flag=0;//计数
		int day=0;//储存日期
		for (IntWritable v : values) {
			//如果计算为0取第一位的年月日:温度
			if(flag==0){
				//设key和value值
				key.set(mytq.getYear()+"-"+mytq.getMonth()+"-"+mytq.getDay()+":");
				value.set(mytq.getWd());
				//给day设值 防止重复天数
				day=mytq.getDay();
				flag++;//计数+1
				context.write(key, value);
			}
			//如果计数为1且日期不和第一次写出的日期不一致
			if(flag!=0&&mytq.getDay()!=day){
				key.set(mytq.getYear()+"-"+mytq.getMonth()+"-"+mytq.getDay()+":");
				value.set(mytq.getWd());
				context.write(key, value);
				break;
			}
		}
	}
}
