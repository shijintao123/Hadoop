package com.sjt.mr.tq.demo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyTQ implements WritableComparable<MyTQ>{

	private int year;//��
	private int month;//��
	private int day;//��
	private int wd;//�¶�
	
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		wd = in.readInt();
	}

	@Override
	public int compareTo(MyTQ mytq) {
		//1980-05-02 34c
		//�Ƚ����Ƿ���ͬ
		int c1 = Integer.compare(year, mytq.getYear());
		if(c1==0){
			//�����ͬ��Ƚ��·�
			int c2 = Integer.compare(month, mytq.getMonth());
			if(c2==0){
				//����·���ͬ��Ƚ��¶�,ʹ���¶Ƚ�������
				return -Integer.compare(wd, mytq.getWd());
			}
			return c2;
		}
		return c1;
	}

}
