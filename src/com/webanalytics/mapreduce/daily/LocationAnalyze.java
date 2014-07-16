package com.webanalytics.mapreduce.daily;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.tomcat.dbcp.dbcp.BasicDataSource;

import com.webanalytics.dto.WebCollectionDTO;
import com.webanalytics.hbase.model.AnalyticTableConstant;
import com.webanalytics.hbase.model.RawDataTable;
import com.webanalytics.util.DateHelper;
import com.webanalytics.util.JAXBContextHelper;

public class LocationAnalyze {

	public static class Mapper extends
			TableMapper<ImmutableBytesWritable, IntWritable> {

		Map<String, Integer> locationCache = new HashMap<String, Integer>();
		Connection conn = null;
		private final String DELIMITER = "$$$";
		@Override
		protected void setup(Context context) {
			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName("com.mysql.jdbc.Driver");
			dataSource.setUsername("pgrwrite");
			dataSource.setPassword("Sravya0828");
			dataSource.setUrl("jdbc:mysql://pgrwrite.db.7322579.hostedresource.com/pgrwrite");
			dataSource.setMaxActive(10);
			dataSource.setMaxIdle(5);
			dataSource.setInitialSize(5);
			try {
				conn = dataSource.getConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		@Override
		protected void cleanup(Context context){
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private Integer getCityNameForIpAddress(String ipAddress){
			String[] split = ipAddress.split("\\.");
			Integer city = locationCache.get(ipAddress);
			if( city == null ){
				city = getFromDb(split[0], split[1], split[2]);
				
				locationCache.put(ipAddress, city);
				
			}
			return city;
		}
		
		private Integer getFromDb(String first,String second,String third){
			Statement statement = null;
			ResultSet rs =  null;
			try {
				 statement = conn.createStatement();
				 rs =  statement.executeQuery("select city from ip4_"+first+" where b = "+second +" and c = "+third);
				 
				 if( rs.next()){
					 return rs.getInt(1);
				 }else{
					 rs.close();
					 rs =  statement.executeQuery("select city from ip4_"+first+" where b = "+second);
					 if( rs.next()){
						 return rs.getInt(1);
					 }else{
						 return null;
					 }
				 }
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				try {
					if( statement != null ){
						statement.close();
					}
					if( rs != null ){
						rs.close();
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return null;
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
				//if( dto.getDataType().equals(WebCollectionDTO.PAGE_STREAM)){
					dbKey = dto.getAppId() + ","
							+ DateHelper.getDateStartInHumanReadable(dto.getTimeCollected());
					Integer code = getCityNameForIpAddress(dto.getRequestedFromIp());
					
					if( code != null ){
						//System.out.println("city code found "+code);
						IntWritable cityKey = new IntWritable( code );
						
						byte[] bKey = Bytes.toBytes(dbKey);
						ImmutableBytesWritable ibKey = new ImmutableBytesWritable(bKey);
						context.write(ibKey, cityKey);
					}else{
						System.out.println("City missed for "+dto.getRequestedFromIp());
					}
					
			//	}
				

			}
			

		}
	}

	public static class Reduce extends
			TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable rowkey,
				Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(rowkey.get());
			Map<Integer, Integer> result = new HashMap<Integer, Integer>();
			for (IntWritable cityCode : values) {
				
				Integer cityCodeValue = result.get(cityCode.get());
				if ( cityCodeValue == null) {
					result.put(cityCode.get(), new Integer(1) );
				} else {
					
					result.put(cityCode.get(), cityCodeValue+1);
				}
				
			}
			for (Entry<Integer, Integer> entry : result.entrySet()) {
				put.add(AnalyticTableConstant.LOCATION_COLUMN_FAMILY,
						Bytes.toBytes(entry.getKey()),
						Bytes.toBytes(entry.getValue()));
			}
			ImmutableBytesWritable ibKey = new ImmutableBytesWritable(rowkey);
			context.write(ibKey, put);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "Location analyze Per Date");
		job.setJarByClass(LocationAnalyze.class);

		Scan scan = new Scan();
		scan.addFamily(RawDataTable.COLUMN_FAMILY);
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(RawDataTable.TABLE_NAME),
				scan, 
				Mapper.class,
				ImmutableBytesWritable.class,
				IntWritable.class, 
				job);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(AnalyticTableConstant.DAILY_TABLE_NAME),
				Reduce.class,
				job);
	//	job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
