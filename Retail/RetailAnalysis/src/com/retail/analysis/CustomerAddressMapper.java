package com.retail.analysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerAddressMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
	//Text variable to hold the address
	private Text address = new Text();
	//LongWritable variable to hold the empNo
	private LongWritable empNo = new LongWritable();
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		String [] records = values.toString().split(",");
		address.set("city"+","+records[2]);
		//setting the output key
		empNo.set(Long.parseLong(records[0]));
		context.write(empNo,address);
	}
}
