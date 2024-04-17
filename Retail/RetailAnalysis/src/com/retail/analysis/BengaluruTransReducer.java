package com.retail.analysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* To compute the number of transactions made by customers at Bengaluru*/

public class BengaluruTransReducer extends Reducer<LongWritable,Text,Text,IntWritable> {
	//Text variable to hold the city name
	private IntWritable transactions = new IntWritable();
	//Text variable to hold the city name
	private Text city = new Text();
	public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		int trans =0;
		for(Text value:values) {
			String [] records =value.toString().split(",");
			if(records[0].equals("sales")) {
				trans+=1;
				//setting the output value
				transactions.set(trans);
			}
			else if(records[0].equals("city")) {
				//emitting the output key
				city.set(records[1]);
				}
			}	
		//emitting the results for the records if the city is bengaluru
		if(city.toString().equals("Bengaluru")) {
			context.write(city, transactions);
		}
	}
}
