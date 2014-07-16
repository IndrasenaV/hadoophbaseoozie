package com.webanalytics.example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExample {
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "mytable");
		
		Get get = new Get(Bytes.toBytes("123456"));
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
		
		Result result = table.get(get);
		
		byte[] val = result.getValue(Bytes.toBytes("cf1"),
				Bytes.toBytes("col1"));
		System.out.println("Value: " + Bytes.toString(val));

		table.close();
		
	}
}