package com.sjt.mr.tq.demo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//����Ƚ�
public class MyGroupComparator extends WritableComparator{
	//���ø���Ĺ��췽��
	public  MyGroupComparator(){
		super(MyTQ.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyTQ tq1=(MyTQ)a;
		MyTQ tq2=(MyTQ)b;
		//�ȱȽ��� �ٱȽ���
		int c1 = Integer.compare(tq1.getYear(), tq2.getYear());
		if(c1==0){
			return Integer.compare(tq1.getMonth(), tq2.getMonth());
		}
		return c1;
	}
}
