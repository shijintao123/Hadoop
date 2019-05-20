package com.sjt.mr.tq.demo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MyMapper extends Mapper<Object, Text, MyTQ, IntWritable>{

	private MyTQ tqKey=new MyTQ();
	
	private IntWritable Tvalue=new IntWritable();
	//key记录的是偏移量，value为一行的数据
	@Override 
	protected void map(Object key, Text value, Mapper<Object, Text, MyTQ, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		try {
			//1980-01-02 18:15:45    34c 
			//根据制表符分割 获得 split[0]=1980-01-02 18:15:45 split[1]=34c 
			String[] splits =value.toString().split("\t");
			//设值key
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(splits[0]);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			//给tqkey设值
			tqKey.setYear(calendar.get(calendar.YEAR));
			tqKey.setMonth(calendar.get(calendar.MONTH)+1);
			tqKey.setDay(calendar.get(calendar.DAY_OF_MONTH));
			int wd = Integer.parseInt(splits[1].substring(0, splits[1].length()-1));
			tqKey.setWd(wd);
			//设值温度
			Tvalue.set(wd);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		context.write(tqKey, Tvalue);
	}
	/*public static void main(String[] args) {
		String[] splits = "1949-10-01 14:21:02	34c".split("\t");
		System.out.println(splits[0]+" ---"+splits[1]);
	}*/
	
}
