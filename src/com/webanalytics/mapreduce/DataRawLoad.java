package com.webanalytics.mapreduce;

import java.io.IOException;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import com.webanalytics.dto.WebCollectionDTO;
import com.webanalytics.hbase.model.RawDataTable;
import com.webanalytics.util.JAXBContextHelper;

public class DataRawLoad {

	public static class MapClass extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		private static final int timeSplitter = 60*1000*10;// Sets 10 mins data in one row
		
		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {

			WebCollectionDTO dto = (WebCollectionDTO) JAXBContextHelper
					.xmlToObject(val.toString(), WebCollectionDTO.class);
			String dbKey = dto.getAppId();
			String clKey = "";
			Long time = 0L;
			if( dto.getTimeCollected() != null){
				time  = dto.getTimeCollected();
			}else{
				time = new Date().getTime();
			}
			dbKey = dbKey+"," + time/timeSplitter;
			clKey = ""+time%timeSplitter;
			byte[] bKey = Bytes.toBytes(dbKey);
			Put put = new Put(bKey);
			put.add(RawDataTable.COLUMN_FAMILY, Bytes.toBytes(clKey), Bytes.toBytes(val.toString()));

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
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		conf.set("xmlinput.start", "<webCollectionDTO>");
		conf.set("xmlinput.end", "</webCollectionDTO>");

		Job job = new Job(conf);
		job.setJobName(DataLoad.class.getName());
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setMapperClass(MapClass.class);// Custom Mapper
		job.setJarByClass(IpAddressDataHBaseLoad.class);
		job.setInputFormatClass(XmlInputFormat.class);

		XmlInputFormat.setInputPaths(job, args[0]);
		FileInputFormat.setInputPaths(job, args[0]);

		// TableMapReduceUtil.initTab
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(RawDataTable.TABLE_NAME),
				IdentityTableReducer.class, job);

		job.setNumReduceTasks(0);
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(RawDataTable.TABLE_NAME)) {
			System.out.println("Ip Address table already exists.");
		} else {
			System.out.println("Creating Ip Address table...");
			HTableDescriptor desc = new HTableDescriptor(
					RawDataTable.TABLE_NAME);
			HColumnDescriptor pageColumnFamily = new HColumnDescriptor(
					RawDataTable.COLUMN_FAMILY);
			desc.addFamily(pageColumnFamily);
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
