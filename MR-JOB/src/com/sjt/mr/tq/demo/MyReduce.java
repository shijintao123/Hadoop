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
		int flag=0;//����
		int day=0;//��������
		for (IntWritable v : values) {
			//�������Ϊ0ȡ��һλ��������:�¶�
			if(flag==0){
				//��key��valueֵ
				key.set(mytq.getYear()+"-"+mytq.getMonth()+"-"+mytq.getDay()+":");
				value.set(mytq.getWd());
				//��day��ֵ ��ֹ�ظ�����
				day=mytq.getDay();
				flag++;//����+1
				context.write(key, value);
			}
			//�������Ϊ1�����ڲ��͵�һ��д�������ڲ�һ��
			if(flag!=0&&mytq.getDay()!=day){
				key.set(mytq.getYear()+"-"+mytq.getMonth()+"-"+mytq.getDay()+":");
				value.set(mytq.getWd());
				context.write(key, value);
				break;
			}
		}
	}
}
