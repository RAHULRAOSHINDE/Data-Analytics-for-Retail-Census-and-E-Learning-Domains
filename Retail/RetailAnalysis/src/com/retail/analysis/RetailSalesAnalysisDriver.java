package com.retail.analysis;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class RetailSalesAnalysisDriver {
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		//This comfiguratiom is to compute avg sales of every customer
		Configuration avgSalesConf=new Configuration();
		Job avgSalesJob = Job.getInstance(avgSalesConf);
		avgSalesJob.setJobName("AverageSalesValue");
		avgSalesJob.setJarByClass(RetailSalesAnalysisDriver.class);
		avgSalesJob.setMapperClass(SalesMapper.class);
		avgSalesJob.setCombinerClass(AvgSalesReducer.class);
		avgSalesJob.setReducerClass(AvgSalesReducer.class);
		avgSalesJob.setOutputKeyClass(LongWritable.class);
		avgSalesJob.setOutputValueClass(Text.class);
		Path retailInputPath = new Path("/user/retail_analysis/RetailData.txt");
		Path avgOutPath = new Path("/user/mapreduce_output/AvgSales");
		FileInputFormat.addInputPath(avgSalesJob, retailInputPath);
		FileOutputFormat.setOutputPath(avgSalesJob,avgOutPath );
		avgSalesJob.waitForCompletion(true);

		//This configuration is for partitioning the sales data 
		Configuration salesPartitionConf=new Configuration();
		Job salesPartitionJob = Job.getInstance(salesPartitionConf);
		salesPartitionJob.setJobName("SalesPartition");
		salesPartitionJob.setJarByClass(RetailSalesAnalysisDriver.class);
		salesPartitionJob.setMapperClass(SalesMapper.class);
		salesPartitionJob.setPartitionerClass(PricePartitioner.class);
		salesPartitionJob.setNumReduceTasks(3);
		salesPartitionJob.setOutputKeyClass(LongWritable.class);
		salesPartitionJob.setOutputValueClass(Text.class);
		Path salesOutputPath = new Path("/user/mapreduce_output/SalesPartitioned");
		FileInputFormat.addInputPath(salesPartitionJob, retailInputPath);
		FileOutputFormat.setOutputPath(salesPartitionJob,salesOutputPath);
		salesPartitionJob.waitForCompletion(true);

		//This configuration is to compute the transactions done at bengaluru
		Configuration customerTransConf = new Configuration();
		Job customerTransJob = Job.getInstance(customerTransConf);
		customerTransJob.setJobName("CustomerTransactions");
		customerTransJob.setJarByClass(RetailSalesAnalysisDriver.class);
		customerTransJob.setMapperClass(PartitionedSalesMapper.class);
		customerTransJob.setMapperClass(CustomerAddressMapper.class);
		customerTransJob.setReducerClass(BengaluruTransReducer.class);
		customerTransJob.setMapOutputKeyClass(LongWritable.class);
		customerTransJob.setMapOutputValueClass(Text.class);
		customerTransJob.setOutputKeyClass(Text.class);
		customerTransJob.setOutputValueClass(IntWritable.class);
		Path salesInputPath = new Path("/user/mapreduce_output/SalesPartitioned");
		Path customerInputPath = new Path("/user/retail_analysis/CustomerAddress.txt");
		Path transOutputPath = new Path("/user/mapreduce_output/CustomerTrans");
		MultipleInputs.addInputPath(customerTransJob,salesInputPath,TextInputFormat.class,PartitionedSalesMapper.class);
		MultipleInputs.addInputPath(customerTransJob,customerInputPath,TextInputFormat.class,CustomerAddressMapper.class);
		FileOutputFormat.setOutputPath(customerTransJob,transOutputPath);
		customerTransJob.waitForCompletion(true);
		
		//compute the sum of total amount of transactions
		Configuration totalAmountConf = new Configuration();
		Job totalAmountJob = Job.getInstance(totalAmountConf);
		totalAmountJob.setJobName("TotalAmountTransactions");
		totalAmountJob.setJarByClass(RetailSalesAnalysisDriver.class);
		totalAmountJob.setMapperClass(CustomerAddressMapper.class);
		totalAmountJob.setMapperClass(PartitionedSalesMapper.class);
		totalAmountJob.setReducerClass(TransactionAmountReducer.class);
		totalAmountJob.setMapOutputKeyClass(LongWritable.class);
		totalAmountJob.setMapOutputValueClass(Text.class);
		totalAmountJob.setOutputKeyClass(Text.class);
		totalAmountJob.setOutputValueClass(FloatWritable.class);
		Path amountOutputPath = new Path("/user/mapreduce_output/CityTotalAmount");
		MultipleInputs.addInputPath(totalAmountJob, customerInputPath, TextInputFormat.class,CustomerAddressMapper.class);
		MultipleInputs.addInputPath(totalAmountJob, salesInputPath, TextInputFormat.class,PartitionedSalesMapper.class);
		FileOutputFormat.setOutputPath(totalAmountJob, amountOutputPath);
		System.exit(totalAmountJob.waitForCompletion(true) ? 0 : 1);
		
	}
}


























