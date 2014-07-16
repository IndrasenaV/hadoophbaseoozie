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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import com.webanalytics.dto.WebCollectionDTO;
import com.webanalytics.hbase.model.AnalyticTableConstant;
import com.webanalytics.util.JAXBContextHelper;


public class DataLoad {


	public static final byte[] TABLE_NAME = Bytes
			.toBytes("STREAM_DATA");

	public static class MapClass extends
			Mapper<LongWritable, Text,ImmutableBytesWritable, Put> {

		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			
//			WebCollectionDTO dto = (WebCollectionDTO)JAXBContextHelper.xmlToObject(val.toString(), WebCollectionDTO.class);
//			byte[] bKey = Bytes.toBytes(dto.getAppId());
//			Put put = new Put(bKey);
//			boolean dataTypePresent = false;
//			if(dto.getDataType().equals(WebCollectionDTO.PAGE_STREAM_TYPE)){
//				dataTypePresent = true;
//				put.add(PageStream.COLUMN_FAMILY, Bytes.toBytes("ipAddress"),
//						Bytes.toBytes(dto.getRequestedFromIp()));
//				put.add(PageStream.COLUMN_FAMILY, Bytes.toBytes("originPage"),
//						Bytes.toBytes(dto.getPage()));
//				put.add(PageStream.COLUMN_FAMILY, Bytes.toBytes("time"),
//						Bytes.toBytes(dto.getTimeCollected()));
//			}else{
//				dataTypePresent = true;
//				put.add(ClickStream.COLUMN_FAMILY, Bytes.toBytes("ipAddress"),
//						Bytes.toBytes(dto.getRequestedFromIp()));
//				put.add(PageStream.COLUMN_FAMILY, Bytes.toBytes("originPage"),
//						Bytes.toBytes(dto.getPage()));
//				put.add(PageStream.COLUMN_FAMILY, Bytes.toBytes("time"),
//						Bytes.toBytes(dto.getTimeCollected()));
//				put.add(PageStream.COLUMN_FAMILY, Bytes.toBytes("clickInformation"),
//						Bytes.toBytes(dto.getClickDivInformation()));
//			}
			
			
//			if( dataTypePresent ){
//
//				ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
//				context.write(ibKey, put);	
		//	}
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
			HColumnDescriptor pageColumnFamily = new HColumnDescriptor(AnalyticTableConstant.BROWSER_COLUMN_FAMILY);
			desc.addFamily(pageColumnFamily);
			HColumnDescriptor clickColumnFamily = new HColumnDescriptor(AnalyticTableConstant.LOCATION_COLUMN_FAMILY);
			desc.addFamily(clickColumnFamily);
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
