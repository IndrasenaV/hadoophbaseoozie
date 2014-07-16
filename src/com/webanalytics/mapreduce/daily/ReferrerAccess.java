package com.webanalytics.mapreduce.daily;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.jruby.util.URLUtil;

import com.webanalytics.dto.WebCollectionDTO;
import com.webanalytics.hbase.model.AnalyticTableConstant;
import com.webanalytics.hbase.model.RawDataTable;
import com.webanalytics.util.DateHelper;
import com.webanalytics.util.JAXBContextHelper;

import eu.bitwalker.useragentutils.UserAgent;

public class ReferrerAccess {

	public static class Mapper extends
			TableMapper<ImmutableBytesWritable, Text> {

		@Override
		protected void setup(Context context) {

		}

		@Override
		protected void map(ImmutableBytesWritable rowkey, Result result,
				Context context) throws IOException, InterruptedException {
			NavigableMap<byte[], byte[]> columnMap = result
					.getFamilyMap(RawDataTable.COLUMN_FAMILY);
			String dbKey = null;

			for (Entry<byte[], byte[]> entry : columnMap.entrySet()) {
				WebCollectionDTO dto = (WebCollectionDTO) JAXBContextHelper
						.xmlToObject(Bytes.toString(entry.getValue()),
								WebCollectionDTO.class);
				dbKey = dto.getAppId() + ","
						+ DateHelper.getDateStartInHumanReadable(dto.getTimeCollected());
				UserAgent userAgent = new UserAgent(dto.getUserAgent());
				Text val = new Text(userAgent.getBrowser().getName());
				
				byte[] bKey = Bytes.toBytes(dbKey);
				ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
				context.write(ibKey, val);
			}

		}
	}

	public static class Reduce
			extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable rowkey,
				Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(rowkey.get());
			Map<String, Integer> result = new HashMap<String, Integer>();
			for (Text temp : values) {
				String str = temp.toString();
				Integer count = result.get(str);
				if( count == null ){
					result.put(str, new Integer(1));
				}else{
					result.put(str, new Integer(count.intValue()+1));
				}
			}
			for (Entry<String, Integer> entry : result.entrySet()) {
				put.add(AnalyticTableConstant.REFERRER_COLUMN_FAMILY,
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
		job.setJarByClass(BrowserAccess.class);

		Scan scan = new Scan();
		scan.addFamily(RawDataTable.COLUMN_FAMILY);
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(RawDataTable.TABLE_NAME), scan, Mapper.class,
				ImmutableBytesWritable.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(AnalyticTableConstant.DAILY_TABLE_NAME), Reduce.class,
				job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
