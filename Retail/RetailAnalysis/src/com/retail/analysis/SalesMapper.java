package com.retail.analysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SalesMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
	//LongWritable variable to hold the empNo
	private LongWritable empNo = new LongWritable();
	//FloatWritable variable to hold the 
	private Text salesValue = new Text();
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		//splitting the records into a array of strings based on a delimiter
		String [] records  = values.toString().split("\t");
		//setting the output key
		empNo.set(Long.parseLong(records[0]));
		//setting the output value
		salesValue.set("sales"+","+Float.parseFloat(records[11]));
		//emitting the results
		context.write(empNo,salesValue);
	}

}
