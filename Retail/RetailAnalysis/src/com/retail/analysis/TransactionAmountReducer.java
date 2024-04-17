package com.retail.analysis;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*To compute sum of total number of transactions carried by every city*/
public class TransactionAmountReducer extends Reducer<LongWritable,Text,Text,FloatWritable> {
	//FloatWritable variable to hold the sum of transactions
	private FloatWritable totalAmount = new FloatWritable();
	//Text variable to hold the city name
	private Text city = new Text();
	public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		float sum = 0;
		for(Text value:values) {
			String [] records = value.toString().split(",");
			if(records[0].equals("sales")) {
				sum = sum + Float.parseFloat(records[1].toString());
			}
			else if(records[0].equals("city")) {
				//setting the output key
				city.set(records[1]);
			}
		}
		//setting the output value
		totalAmount.set(sum);
		//emitting the results
		context.write(city,totalAmount);
	}
}
