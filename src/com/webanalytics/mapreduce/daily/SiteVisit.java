package com.webanalytics.mapreduce.daily;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.webanalytics.dto.WebCollectionDTO;
import com.webanalytics.hbase.model.AnalyticTableConstant;
import com.webanalytics.hbase.model.RawDataTable;
import com.webanalytics.util.DateHelper;
import com.webanalytics.util.JAXBContextHelper;

public class SiteVisit {

	public static class Mapper extends
			TableMapper<ImmutableBytesWritable, MapWritable> {

		@Override
		protected void setup(Context context) {

		}

		@Override
		protected void map(ImmutableBytesWritable rowkey, Result result,
				Context context) throws IOException, InterruptedException {
			NavigableMap<byte[], byte[]> columnMap = result
					.getFamilyMap(RawDataTable.COLUMN_FAMILY);
			String dbKey = null;
			MapWritable tempMap = new MapWritable();
			
			for (Entry<byte[], byte[]> entry : columnMap.entrySet()) {
				WebCollectionDTO dto = (WebCollectionDTO) JAXBContextHelper
						.xmlToObject(Bytes.toString(entry.getValue()),
								WebCollectionDTO.class);
				dbKey = dto.getAppId() + ","
						+ DateHelper.getDateStartInHumanReadable(dto.getTimeCollected());
				Text clKey = new Text(dto.getPage());
				if (tempMap.get(clKey) == null) {
					tempMap.put(clKey, new IntWritable(1));
				} else {
					tempMap.put(clKey, new IntWritable(((IntWritable)tempMap.get(clKey)).get()+ 1));
				}

			}
			if (dbKey != null) {
				byte[] bKey = Bytes.toBytes(dbKey);
				ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
				context.write(ibKey, tempMap);

			}

		}
	}

	public static class Reduce extends
			TableReducer<ImmutableBytesWritable, MapWritable, ImmutableBytesWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable rowkey,
				Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(rowkey.get());
			Map<String, Integer> result = new HashMap<String, Integer>();
			for (MapWritable temp : values) {
				for (Entry temp1 : temp.entrySet()) {
					Text text = (Text)temp1.getKey();
					IntWritable intWritable = (IntWritable)temp1.getValue();
					if (result.get(text.toString()) == null) {
						result.put(text.toString(), intWritable.get());
					} else {
						Integer pageHit = intWritable.get()
								+ result.get(text.toString());
						result.put(text.toString(), pageHit);
					}
				}
			}
			for (Entry<String, Integer> entry : result.entrySet()) {
				put.add(AnalyticTableConstant.PAGEHIT_COLUMN_FAMILY,
						Bytes.toBytes(entry.getKey()),
						Bytes.toBytes(entry.getValue()));
			}
			ImmutableBytesWritable ibKey = new ImmutableBytesWritable(rowkey);
			context.write(ibKey, put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "Site Visit Per Date");
		job.setJarByClass(SiteVisit.class);

		Scan scan = new Scan();
		scan.addFamily(RawDataTable.COLUMN_FAMILY);
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(RawDataTable.TABLE_NAME),
				scan, 
				Mapper.class,
				ImmutableBytesWritable.class,
				MapWritable.class, 
				job);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(AnalyticTableConstant.DAILY_TABLE_NAME),
				Reduce.class,
				job);
	//	job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
