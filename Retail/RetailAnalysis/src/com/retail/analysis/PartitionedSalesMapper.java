package com.retail.analysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionedSalesMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
	//LongWritable variable to hold the empNo
	private LongWritable empNo = new LongWritable();
	//Text variable to hold the empsalesvalue
	private Text salesValue =  new Text();
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		String [] records = values.toString().split("\t");
		//setting the output key
		empNo.set(Long.parseLong(records[0]));
		//setting the output value
		salesValue.set(records[1]);
		//emitting the reults
		context.write(empNo,salesValue);
	}
}
