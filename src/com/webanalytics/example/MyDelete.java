package com.webanalytics.example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Delete;

public class MyDelete {
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "mytable");

		Delete delete = new Delete(Bytes.toBytes("123456"));
		delete.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));

		table.delete(delete);
		table.close();

	}

}