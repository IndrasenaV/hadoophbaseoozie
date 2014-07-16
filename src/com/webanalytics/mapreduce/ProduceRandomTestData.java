package com.webanalytics.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class ProduceRandomTestData {
	
	private static List<String> randomIpAddress = new ArrayList<String>();
	private static List<String> appIds = new ArrayList<String>();
	static Random rand = new Random();
	static{
		for( int i=0;i<1000;i++){
			String str = rand.nextInt(255)+"."+rand.nextInt(255)+"."+rand.nextInt(255)+"."+rand.nextInt(255);
			randomIpAddress.add(str);
		}
		appIds.add("1111111111");
		appIds.add("2222222222");
		appIds.add("3333333333");
		appIds.add("4444444444");
		appIds.add("5555555555");
	}
	
	public static class MapClass extends Mapper<LongWritable, Text, LongWritable, Text>  {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if( line.contains("111111222222")){
				line = line.replaceAll("111111222222", appIds.get(rand.nextInt(5)));
				String oldIpAddress = line.split("\t")[2];
				line = line.replaceAll(oldIpAddress, randomIpAddress.get(rand.nextInt(1000)));
				context.write(key, new Text(line));
			}
			
			
		}

	}

	 public static class Reduce extends Reducer<LongWritable, Text, Text, Text>  {

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			      throws IOException, InterruptedException  {
			
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext() ){
			//	iterator.next().toString().split("\t");
				context.write(new Text(""), iterator.next());
			}
			
		}

	}


	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, ProduceRandomTestData.class.getName());
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path outputPath = new Path(args[1]);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			System.out.println("Deleting output path before proceeding.");
			fs.delete(outputPath, true);
		}
	    job.waitForCompletion(true);
	}
}
