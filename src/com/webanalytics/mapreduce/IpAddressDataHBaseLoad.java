package com.webanalytics.mapreduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class IpAddressDataHBaseLoad {

	public static final byte[] TABLE_NAME = Bytes
			.toBytes("ipaddresstolocation");
	public static final byte[] INFO_FAM = Bytes.toBytes("columnFamily1");

	public static class MapClass extends
			Mapper<LongWritable, Text,ImmutableBytesWritable, Put> {

		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			System.out.println("Entered into method");
			
			Configuration config = context.getConfiguration();
			String[] strs = val.toString().split(",");
			String family = config.get("");
			String column = strs[1];
			String value = strs[1];
			String sKey = "";
			byte[] bKey = Bytes.toBytes(sKey);
			Put put = new Put(bKey);
			put.add(Bytes.toBytes(family), Bytes.toBytes(column),
					Bytes.toBytes(value));

			ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
			context.write(ibKey, put);
		}

	}

	public static class Reduce extends
			TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable rowkey,
				Iterable<Put> values, Context context) {
			Iterator<Put> i = values.iterator();
			if (i.hasNext()) {
				try {
					context.write(rowkey, i.next());
				} catch (Exception e) {
					// gulp!
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = new Job(conf);
		job.setJobName(IpAddressDataHBaseLoad.class.getName());
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setMapperClass(MapClass.class);// Custom Mapper
		job.setJarByClass(IpAddressDataHBaseLoad.class);
		job.setInputFormatClass(TextInputFormat.class);
	//	job.setOutputFormatClass(HFileOutputFormat.class);

		TextInputFormat.setInputPaths(job, args[0]);
		// TableMapReduceUtil.initTab
		TableMapReduceUtil.initTableReducerJob(
			      Bytes.toString(TABLE_NAME),
			      IdentityTableReducer.class,
			      job);

		job.setNumReduceTasks(0);
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(TABLE_NAME)) {
			System.out.println("Ip Address table already exists.");
		} else {
			System.out.println("Creating Ip Address table...");
			HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor c = new HColumnDescriptor(INFO_FAM);
			desc.addFamily(c);
			admin.createTable(desc);
			System.out.println("Ip Address table created.");
		}
		admin.close();
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job");
		}
	}
}
