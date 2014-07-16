package com.webanalytics.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class PutExample {
	public static void main(String[] args) throws IOException {
		
		//create 'mytable', 'cf1'
		
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "mytable");
		Put put = new Put(Bytes.toBytes("123456"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"),
				Bytes.toBytes("val12345"));

		table.put(put);
		
		table.close();
	}
}
