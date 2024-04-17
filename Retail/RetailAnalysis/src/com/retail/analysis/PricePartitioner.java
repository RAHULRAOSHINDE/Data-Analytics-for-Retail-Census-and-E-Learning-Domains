package com.retail.analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/* To partition the sales data with prices less than 10000, 
between 20000 and 40000, greater than 40000*/

public class PricePartitioner extends Partitioner<LongWritable,Text> {
	@Override
	public int getPartition(LongWritable key, Text values, int numReduceTasks) {
		String [] records = values.toString().split(",");
		float price = 0;
		if(records[0].equals("sales")) {
			price = Float.parseFloat(records[1]);
		}
		if(numReduceTasks ==0) {
			return 0;
		}
		if(price < 10000) {
			return 0;
		}
		else if(price >= 20000 && price <= 40000) {
			return 1 % numReduceTasks;
		}
		else {
			return 2 % numReduceTasks;
		}
	}


}
