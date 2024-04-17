package com.retail.analysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* To Compute the average sales value of every customer*/

public class AvgSalesReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
	//FloatWritable variable to hold the average sales value 
	private Text averageSaleValue = new Text();
	public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		float totalPrice=0;
		int count= 0;
		//Iterating over all the records and calculating the average sales value
		for(Text  value:values) {
			String [] records = value.toString().split(",");
			totalPrice= totalPrice+Float.parseFloat(records[1]);
			count+=1;
		}
		//setting the output value
		averageSaleValue.set("avg"+","+Float.toString(totalPrice/count));
		//emitting the results
		context.write(key,averageSaleValue);
	}
}
